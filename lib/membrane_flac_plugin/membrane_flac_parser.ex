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
              case maybe_set_buffer_pts(buf, state) do
                {:ok, buffer} ->
                  {:buffer, {:output, buffer}}
                {:meta_buffer, buffer} ->
                  {:meta_buffer, {:output, buffer}}
              end
          end)
        # now we have actions list with maybe streamformat, maybe some audio and maybe some meta
        # if streamformat is found anywhere it has to be always at the start of actions list
        {actions_filtered, state} = cond do

          # if there is only meta, we push all meta buffers to state.meta_queue and return empty actions
          (List.keymember?(actions, :stream_format, 0) or List.keymember?(actions, :meta_buffer, 0)) and not List.keymember?(actions, :buffer, 0) ->
            state = Map.update!(state, :meta_queue, fn meta_queue ->
              meta_queue ++ actions
            end)
            {[], state}

          # if there is meta & audio, we insert at the start any remaining meta from state.meta_queue
          # and set pts of all meta to pts of first audio next to meta and return all
          (List.keymember?(actions, :meta_buffer, 0) or List.keymember?(actions, :meta_buffer, 0)) and List.keymember?(actions, :buffer, 0) ->
            meta_buffers_count = Enum.count(actions, fn action ->
              {key, _rest} = action
              key != :buffer
            end)
            {only_meta, only_audio} = Enum.split(actions, meta_buffers_count)
            {:buffer, {:output, audio_buffer}} = List.first(only_audio)
            meta_buffers_with_pts =
              state.meta_queue ++ only_meta |> Enum.map(fn
                {:stream_format, {:output, %FLAC{}}} = format -> format
                {:meta_buffer, {:output, %Buffer{} = buffer}} ->
                  {:buffer, {:output, %Buffer{buffer | pts: audio_buffer.pts}}}
            end)
            {meta_buffers_with_pts ++ only_audio, %{state | meta_queue: []}}

          # if there is only audio we insert at the start any remaining meta from state.meta_queue,
          # and set pts of all meta to pts of first audio next to meta and return all
          not (List.keymember?(actions, :meta_buffer, 0) or List.keymember?(actions, :meta_buffer, 0)) and List.keymember?(actions, :buffer, 0) ->
            if state.meta_queue == [] do
              {actions, state}
            else
              {:buffer, {:output, audio_buffer}} = List.first(actions)
              meta_buffers_with_pts =
                state.meta_queue |> Enum.map(fn
                  {:stream_format, {:output, %FLAC{}}} = format -> format
                  {:meta_buffer, {:output, %Buffer{} = buffer}} ->
                    {:buffer, {:output, %Buffer{buffer | pts: audio_buffer.pts}}}
              end)
              {meta_buffers_with_pts ++ actions, %{state | meta_queue: []}}
            end

          # actions are empty (maybe, idk), nothing was parsed
          true ->
            {actions, state}
        end
        IO.inspect(actions_filtered, label: "actions_filtered")

        {actions_filtered, %{state | parser: parser}}

      {:error, reason} ->
        raise "Parsing error: #{inspect(reason)}"
    end
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    {:ok, buffer} = Engine.flush(state.parser)

    actions = [
      buffer: {:output, buffer}, # fix this
      end_of_stream: :output,
      notify_parent: {:end_of_stream, :input}
    ]

    {actions, state}
  end
  # maybe_set_buffer_pts() does not take into account changing sample rate during stream, because parser engine doesn't support it.
  # If you add support for it in parser engine, this function also need to be updated.
  defp maybe_set_buffer_pts(buffer, state) do
    if state.generate_best_effort_timestamps? do
        case buffer.metadata do
          %{sample_rate: sample_rate, starting_sample_number: starting_sample_number} ->
            pts = Ratio.new(starting_sample_number, sample_rate) |> Time.seconds()
            {:ok, %Buffer{buffer | pts: pts}}

          _no_audio_metadata ->
            {:meta_buffer, buffer}
        end

    else
      {:ok, {%Buffer{buffer | pts: state.input_pts}, state}}
    end
  end

end
