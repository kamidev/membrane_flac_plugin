defmodule Membrane.FLAC.Parser do
  @moduledoc """
  An element parsing FLAC encoded audio stream.

  Wraps `Membrane.FLAC.Parser.Engine`, see its docs for more info.
  """
  use Membrane.Filter
  require Membrane.Logger
  alias Membrane.{Buffer, FLAC, Time}
  alias Membrane.FLAC.Parser.Engine

  def_output_pad :output, accepted_format: FLAC

  def_input_pad :input,
    accepted_format: %Membrane.RemoteStream{content_format: format} when format in [FLAC, nil]

  def_options streaming?: [
                description: """
                This option set to `true` allows parser to accept FLAC stream,
                e.g. only frames without header
                """,
                default: false,
                spec: boolean()
              ],
              generate_best_effort_timestamps?: [
                spec: boolean(),
                default: false,
                description: """
                If this is set to true parser will try to generate timestamps for every frame based on sample count and sample rate,
                otherwise it will pass pts from input to output, even if it's nil.
                """
              ]

  @impl true
  def handle_init(_ctx, opts) do
    {[],
     opts
     |> Map.from_struct()
     |> Map.merge(%{
       parser: nil,
       input_pts: nil,
       current_pts: nil,
       meta_queue: [],
       frame_duration: nil
     })}
  end

  @impl true
  def handle_playing(_ctx, %{streaming?: streaming?} = state) do
    state = %{state | parser: Engine.init(streaming?)}
    {[], state}
  end

  @impl true
  def handle_stream_format(:input, _format, _ctx, state) do
    {[], state}
  end

  defp set_current_pts(%{generate_best_effort_timestamps?: true} = state, _input_pts) do
    state
  end

  defp set_current_pts(
         %{generate_best_effort_timestamps?: false, meta_queue: [_ | _]} = state,
         input_pts
       ) do
    %{state | current_pts: input_pts}
  end

  defp set_current_pts(
         %{generate_best_effort_timestamps?: false, parser: %{queue: <<>>}} = state,
         input_pts
       ) do
    %{state | current_pts: input_pts}
  end

  defp set_current_pts(state, _input_pts), do: state
  @impl true
  def handle_buffer(
        :input,
        %Buffer{payload: payload, pts: input_pts},
        _ctx,
        %{parser: parser} = state
      ) do
    state = set_current_pts(state, input_pts)
    state = %{state | input_pts: input_pts}

    validate_pts_integrity(state)

    case Engine.parse(payload, parser) do
      {:ok, results, parser} ->
        actions =
          results
          |> Enum.map(fn
            %FLAC{} = format ->
              {:stream_format, {:output, format}}

            %Buffer{} = buf ->
              {:buffer, {:output, buf}}
          end)

        {actions, state} = calculate_pts(actions, state)

        {actions, %{state | parser: parser}}

      {:error, reason} ->
        raise "Parsing error: #{inspect(reason)}"
    end
  end

  defp validate_pts_integrity(state)
       when not state.generate_best_effort_timestamps? and
              state.parser.queue != <<>> and
              state.meta_queue == [] and
              state.current_pts != nil and
              state.input_pts != nil and
              state.frame_duration != nil do
    epsilon = state.frame_duration / 10

    if state.input_pts < state.current_pts - epsilon do
      Membrane.Logger.warning("PTS values are overlapping")
    end

    if state.input_pts > state.current_pts + epsilon do
      Membrane.Logger.warning("PTS values are not continous")
    end

    :ok
  end

  defp validate_pts_integrity(_state), do: :ok

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    {:ok, buffer} = Engine.flush(state.parser)

    {buffers_with_pts, state} = calculate_pts({:buffer, {:output, buffer}}, state)

    actions =
      buffers_with_pts ++
        [
          end_of_stream: :output,
          notify_parent: {:end_of_stream, :input}
        ]

    {actions, state}
  end

  defp calculate_pts(actions, state) do
    # in some cases actions are not a list but a single action tuple
    actions =
      if is_list(actions) do
        actions
      else
        [actions]
      end

    # separate audio buffers from FLAC metadata buffers
    {meta_buffers, audio_buffers} =
      actions
      |> Enum.split_with(fn
        {:stream_format, {:output, %FLAC{}}} ->
          true

        {:buffer, {:output, %Buffer{} = buffer}} ->
          case buffer.metadata do
            %Membrane.FLAC.FrameMetadata{} ->
              false

            _no_metadata ->
              true
          end
      end)

    # set pts to audio buffers
    {audio_buffers, state} = set_pts_audio_buffers(audio_buffers, state)

    # set pts to metadata buffers
    meta_queue = state.meta_queue ++ meta_buffers

    cond do
      audio_buffers != [] and meta_queue != [] ->
        {:buffer, {:output, %Membrane.Buffer{pts: first_valid_pts}}} = List.first(audio_buffers)
        meta_queue_with_pts = set_pts_meta_buffers(meta_queue, first_valid_pts)

        {meta_queue_with_pts ++ audio_buffers, %{state | meta_queue: []}}

      audio_buffers == [] and meta_queue != [] ->
        {[], %{state | meta_queue: meta_queue}}

      true ->
        {audio_buffers, state}
    end
  end

  # set_pts_audio_buffers() does not take into account changing sample rate during stream, because parser engine doesn't support it.
  # If you add support for it in parser engine, this function also need to be updated.
  defp set_pts_audio_buffers(actions, state)
       when state.generate_best_effort_timestamps? == true do
    {Enum.map(actions, fn {:buffer, {:output, buffer}} ->
       %{sample_rate: sample_rate, starting_sample_number: starting_sample_number} =
         buffer.metadata

       pts = Ratio.new(starting_sample_number, sample_rate) |> Time.seconds()
       {:buffer, {:output, %Buffer{buffer | pts: pts}}}
     end), state}
  end

  defp set_pts_audio_buffers(actions, state)
       when state.generate_best_effort_timestamps? == false and state.current_pts != nil do
    %{current_pts: current_pts, buffers: buffers, frame_duration: frame_duration} =
      Enum.reduce(
        actions,
        %{current_pts: state.current_pts, buffers: [], frame_duration: nil},
        fn {:buffer, {:output, buffer}}, acc ->
          %{current_pts: current_pts, buffers: buffers} = acc
          %{sample_rate: sample_rate, samples: samples} = buffer.metadata
          duration = Ratio.new(samples, sample_rate) |> Time.seconds()

          %{
            current_pts: current_pts + duration,
            buffers: buffers ++ [{:buffer, {:output, %Buffer{buffer | pts: current_pts}}}],
            frame_duration: duration
          }
        end
      )

    {buffers, %{state | current_pts: current_pts, frame_duration: frame_duration}}
  end

  defp set_pts_audio_buffers(actions, state)
       when state.generate_best_effort_timestamps? == false and state.current_pts == nil,
       do: {actions, state}

  defp set_pts_meta_buffers(meta_buffers, pts) do
    Enum.map(meta_buffers, fn action ->
      case action do
        {:stream_format, {:output, %FLAC{}}} ->
          action

        {:buffer, {:output, %Buffer{} = buffer}} ->
          {:buffer, {:output, %Buffer{buffer | pts: pts}}}
      end
    end)
  end
end
