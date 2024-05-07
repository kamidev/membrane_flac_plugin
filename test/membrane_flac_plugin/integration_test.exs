defmodule ParsingPipeline do
  @moduledoc false

  alias Membrane.Testing.Pipeline

  @spec make_pipeline(String.t(), String.t(), boolean(), pid()) :: GenServer.on_start()
  def make_pipeline(in_path, out_path, streaming?, pid \\ self()) do
    import Membrane.ChildrenSpec

    links = [
      child(:file_src, %Membrane.File.Source{location: in_path})
      |> child(:parser, %Membrane.FLAC.Parser{streaming?: streaming?})
      |> child(:sink, %Membrane.File.Sink{location: out_path})
    ]

    Pipeline.start_link_supervised!(
      spec: links,
      test_process: pid
    )
  end
end

defmodule Membrane.FLAC.Parser.IntegrationTest do
  use ExUnit.Case
  import Membrane.Testing.Assertions
  alias Membrane.{Pipeline, Time}

  defp prepare_files(filename) do
    in_path = "../fixtures/#{filename}.flac" |> Path.expand(__DIR__)
    out_path = "/tmp/parsed-#{filename}.flac"
    File.rm(out_path)
    on_exit(fn -> File.rm(out_path) end)
    {in_path, out_path}
  end

  defp assert_parsing_success(filename, streaming?) do
    {in_path, out_path} = prepare_files(filename)

    assert pid = ParsingPipeline.make_pipeline(in_path, out_path, streaming?)

    # Wait for EndOfStream message
    assert_end_of_stream(pid, :sink, :input, 3000)
    src_data = File.read!(in_path)
    out_data = File.read!(out_path)
    assert src_data == out_data
    assert Pipeline.terminate(pid) == :ok
  end

  defp assert_parsing_failure(filename, streaming?) do
    {in_path, out_path} = prepare_files(filename)
    Process.flag(:trap_exit, true)

    assert pid = ParsingPipeline.make_pipeline(in_path, out_path, streaming?)

    assert_receive {:EXIT, ^pid, reason}, 3000

    assert {:membrane_child_crash, :parser, {%RuntimeError{message: message}, _stacktrace}} =
             reason

    assert String.starts_with?(message, "Parsing error")
  end

  @moduletag :capture_log

  test "parse whole 'noise.flac' file" do
    assert_parsing_success("noise", false)
  end

  test "parse whole 'noise.flac' file in streaming mode" do
    assert_parsing_success("noise", true)
  end

  test "parse streamed file (only frames, no headers) in streaming mode" do
    assert_parsing_success("only_frames", true)
  end

  test "fail when parsing streamed file (only frames, no headers) without streaming mode" do
    assert_parsing_failure("only_frames", false)
  end

  test "fail when parsing file with junk at the end without streaming mode" do
    assert_parsing_failure("noise_and_junk", false)
  end

  test "generate_best_effort_timestamps false" do
    pipeline = prepare_pts_test_pipeline(false, true)
    assert_start_of_stream(pipeline, :sink)


    Enum.each(0..31, fn _x ->
      assert_sink_buffer(pipeline, :sink, %Membrane.Buffer{pts: out_pts})
      # assert out_pts == 0 |> Time.nanoseconds()
      # IO.inspect(out_pts)
    end)

    # Enum.each(0..3, fn _x ->
    #   assert_sink_buffer(pipeline, :sink, %Membrane.Buffer{pts: out_pts})
    #   # assert out_pts == 0 |> Time.nanoseconds()
    #   # IO.inspect(out_pts, label: "TEST out pts")
    # end)
    # # IO.puts("----")
    # # below we test pts of actual audio buffers
    # Enum.each(0..27, fn index ->
    #   assert_sink_buffer(pipeline, :sink, %Membrane.Buffer{pts: out_pts})
    #   # assert out_pts == (index * 72_000_000) |> Time.nanoseconds()
    #   # IO.inspect(out_pts, label: "TEST out pts")
    # end)

    assert_end_of_stream(pipeline, :sink)
    Pipeline.terminate(pipeline)
  end

  test "generate_best_effort_timestamps true" do
    pipeline = prepare_pts_test_pipeline(true, false)
    assert_start_of_stream(pipeline, :sink)

    # first few buffers contain only FLAC file metadata and are expected to have the same pts as the first buffer containing audio, in this case 0
    Enum.each(0..3, fn _x ->
      assert_sink_buffer(pipeline, :sink, %Membrane.Buffer{pts: out_pts})
      assert out_pts == 0 |> Time.nanoseconds()
    end)
    # below we test pts of actual audio buffers
    Enum.each(0..27, fn index ->
      assert_sink_buffer(pipeline, :sink, %Membrane.Buffer{pts: out_pts})
      assert out_pts == (index * 72_000_000) |> Time.nanoseconds()
    end)

    assert_end_of_stream(pipeline, :sink)
    Pipeline.terminate(pipeline)
  end

  test "generate_best_effort_timestamps false, no input pts" do
    pipeline = prepare_pts_test_pipeline(false, false)
    assert_start_of_stream(pipeline, :sink)

    Enum.each(0..31, fn _index ->
      assert_sink_buffer(pipeline, :sink, %Membrane.Buffer{pts: out_pts})
      assert out_pts == nil
    end)

    assert_end_of_stream(pipeline, :sink)
    Pipeline.terminate(pipeline)
  end

  defp prepare_pts_test_pipeline(generate_best_effort_timestamps?, with_pts?) do
    import Membrane.ChildrenSpec

    spec =
      child(:source, %Membrane.Testing.Source{output: buffers_from_file(with_pts?)})
      |> child(:parser, %Membrane.FLAC.Parser{
        generate_best_effort_timestamps?: generate_best_effort_timestamps?
      })
      |> child(:sink, Membrane.Testing.Sink)

    Membrane.Testing.Pipeline.start_link_supervised!(spec: spec)
  end

  defp buffers_from_file(with_pts?) do
    binary = "../fixtures/noise.flac" |> Path.expand(__DIR__) |> File.read!()

    split_binary(binary)
    |> Enum.with_index()
    |> Enum.map(fn {payload, index} ->
      %Membrane.Buffer{
        payload: payload,
        pts:
          if with_pts? do
            index * 72000000 |> Time.nanoseconds()
          else
            nil
          end
      }
    end)
    # |> IO.inspect()
  end

  @spec split_binary(binary(), list(binary())) :: list(binary())
  def split_binary(binary, acc \\ [])

  def split_binary(<<binary::binary-size(2048), rest::binary>>, acc) do
    split_binary(rest, [binary] ++ acc)
  end

  def split_binary(rest, acc) when byte_size(rest) <= 2048 do
    Enum.reverse(acc) ++ [rest]
  end
end
