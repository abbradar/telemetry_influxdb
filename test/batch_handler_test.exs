defmodule TelemetryInfluxDB.BatchHandlerTest do
  use ExUnit.Case, async: true
  alias TelemetryInfluxDB.BatchHandler

  test "handles formatting multiple events" do
    defmodule MockPublish do
      @behaviour TelemetryInfluxDB.Publisher

      @impl TelemetryInfluxDB.Publisher
      def publish(events, config) do
        send(self(), {:published, events, config})
      end

      @impl TelemetryInfluxDB.Publisher
      def add_config(_), do: :noop
    end

    config = %{
      token: "testing123",
      publisher: MockPublish,
      tags: %{},
      metadata_tag_keys: []
    }

    batch = [
      {[:event1], 1, %{}, %{}, config},
      {[:event2], 2, %{}, %{}, config},
      {[:event3], 3, %{}, %{}, config}
    ]

    BatchHandler.handle_batch(batch)

    expected_message = "event1  1\nevent2  2\nevent3  3"
    assert_receive {:published, ^expected_message, ^config}
  end
end
