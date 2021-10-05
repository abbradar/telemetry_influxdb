defmodule TelemetryInfluxDB.BatchReporterTest do
  use ExUnit.Case, async: false

  alias TelemetryInfluxDB.BatchReporter

  test "batches immediate events" do
    test_pid = self()

    report_fn = fn events ->
      send(test_pid, {:test_report, events})
    end

    reporter = start_reporter(report_fn: report_fn)
    [event1, event2, event3] = random_events(3)

    BatchReporter.enqueue_event(reporter, event1)
    BatchReporter.enqueue_event(reporter, event2)
    BatchReporter.enqueue_event(reporter, event3)

    assert_receive {:test_report, [^event1, ^event2, ^event3]}
  end

  test "waits for batch to fill when delay is enabled" do
    test_pid = self()

    report_fn = fn events ->
      send(test_pid, {:test_report, events})
    end

    reporter = start_reporter(report_fn: report_fn, batch_time: 2)

    [event1, event2, event3, event4, event5, event6] = random_events(6)

    BatchReporter.enqueue_event(reporter, event1)
    Process.sleep(1)
    BatchReporter.enqueue_event(reporter, event2)
    BatchReporter.enqueue_event(reporter, event3)
    Process.sleep(2)
    BatchReporter.enqueue_event(reporter, event4)
    BatchReporter.enqueue_event(reporter, event5)
    BatchReporter.enqueue_event(reporter, event6)

    assert_receive {:test_report, [^event1, ^event2, ^event3]}
    assert_receive {:test_report, [^event4, ^event5, ^event6]}
  end

  test "doesn't wait for batch to fill if events come in slowly" do
    test_pid = self()

    report_fn = fn events ->
      send(test_pid, {:test_report, events})
    end

    reporter = start_reporter(report_fn: report_fn)

    [event1, event2, event3, event4, event5, event6] = random_events(6)

    BatchReporter.enqueue_event(reporter, event1)
    Process.sleep(1)
    BatchReporter.enqueue_event(reporter, event2)
    BatchReporter.enqueue_event(reporter, event3)
    Process.sleep(1)
    BatchReporter.enqueue_event(reporter, event4)
    BatchReporter.enqueue_event(reporter, event5)
    BatchReporter.enqueue_event(reporter, event6)

    assert_receive {:test_report, [^event1]}
    assert_receive {:test_report, [^event2, ^event3]}
    assert_receive {:test_report, [^event4, ^event5, ^event6]}
  end

  test "does not call report function again if there are no remaining events to be reported" do
    test_pid = self()

    report_fn = fn events ->
      send(test_pid, {:test_report, events})
    end

    reporter = start_reporter(report_fn: report_fn)
    event = random_event()

    BatchReporter.enqueue_event(reporter, event)

    assert_receive {:test_report, [^event]}
    refute_receive {:test_report, _}
  end

  test "does not try to report new events that come in while reporting is in progress" do
    test_pid = self()

    report_fn = fn events ->
      # simulate reporting delay
      Process.sleep(1)
      send(test_pid, {:test_report, events})
    end

    reporter = start_reporter(report_fn: report_fn)

    [event1, event2] = random_events(2)

    BatchReporter.enqueue_event(reporter, event1)
    BatchReporter.enqueue_event(reporter, event2)

    assert_receive {:test_report, [^event1, ^event2]}
    refute_receive {:test_report, []}
  end

  defp start_reporter(options) do
    name = "BatchReporter#{random_number()}" |> String.to_atom()
    options = Keyword.merge([name: name], options)

    {:ok, reporter} = BatchReporter.start_link(options)

    reporter
  end

  defp random_number do
    Enum.random(1..1000) |> to_string()
  end

  defp random_events(count), do: Enum.map(1..count, fn _ -> random_event() end)

  defp random_event, do: "event #{random_number()}"
end
