defmodule Membrane.FLAC.Parser do
  @moduledoc """
  An element parsing FLAC encoded audio stream.

  Wraps `Membrane.FLAC.Parser.Engine`, see its docs for more info.
  """
  use Membrane.Filter

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
    {[], opts |> Map.from_struct() |> Map.merge(%{parser: nil, input_pts: nil, meta_queue: []})}
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

  @impl true
  def handle_buffer(
        :input,
        %Buffer{payload: payload, pts: input_pts},
        _ctx,
        %{parser: parser} = state
      ) do
    state = %{state | input_pts: input_pts}

    case Engine.parse(payload, parser) do
      {:ok, results, parser} ->
        actions =
          results
          |> Enum.map(fn
            %FLAC{} = format ->
              {:stream_format, {:output, format}}

            %Buffer{} = buf ->
              {:buffer, {:output, maybe_generate_buffer_pts(buf, state)}}
          end)

        {actions, state} = wait_for_valid_pts(actions, state)

        {actions, %{state | parser: parser}}

      {:error, reason} ->
        raise "Parsing error: #{inspect(reason)}"
    end
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    {:ok, buffer} = Engine.flush(state.parser)

    queued_meta_buffers =
      if state.generate_best_effort_timestamps? do
        set_buffers_pts(state.meta_queue, 0)
      else
        state.meta_queue
      end

    actions = [
      buffer: {:output, maybe_generate_buffer_pts(buffer, state)},
      end_of_stream: :output,
      notify_parent: {:end_of_stream, :input}
    ]

    {queued_meta_buffers ++ actions, state}
  end

  defp wait_for_valid_pts(actions, state) do
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

    meta_queue = state.meta_queue ++ meta_buffers

    cond do
      audio_buffers != [] and meta_queue != [] ->
        {:buffer, {:output, %Membrane.Buffer{pts: first_valid_pts}}} = List.first(audio_buffers)

        meta_queue_with_pts = set_buffers_pts(meta_queue, first_valid_pts)

        {meta_queue_with_pts ++ audio_buffers, %{state | meta_queue: []}}

      audio_buffers == [] and meta_queue != [] ->
        {[], %{state | meta_queue: meta_queue}}

      true ->
        {audio_buffers, state}
    end
  end

  # maybe_generate_buffer_pts() does not take into account changing sample rate during stream, because parser engine doesn't support it.
  # If you add support for it in parser engine, this function also need to be updated.
  defp maybe_generate_buffer_pts(buffer, state) do
    if state.generate_best_effort_timestamps? do
      case buffer.metadata do
        %{sample_rate: sample_rate, starting_sample_number: starting_sample_number} ->
          pts = Ratio.new(starting_sample_number, sample_rate) |> Time.seconds()
          %Buffer{buffer | pts: pts}

        _no_audio_metadata ->
          buffer
      end
    else
      %Buffer{buffer | pts: state.input_pts}
    end
  end

  defp set_buffers_pts(meta_buffers, pts) do
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
