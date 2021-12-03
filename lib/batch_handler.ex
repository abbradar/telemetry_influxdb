defmodule TelemetryInfluxDB.BatchHandler do
  alias TelemetryInfluxDB.Formatter

  def handle_batch(batch) do
    config = batch_config(batch)
    publisher = config.publisher

    batch
    |> Enum.map(&preprocess_event/1)
    |> Enum.join("\n")
    |> publisher.publish(config)
  end

  def preprocess_event({event, raw_timestamp, measurements, metadata, config}) do
    event_timestamp =
      case raw_timestamp do
        %DateTime{} ->
          raw_timestamp

        ts when is_integer(ts) ->
          case Map.get(metadata, :timestamp_unit, :native) do
            :nanosecond -> raw_timestamp
            timestamp_unit -> System.convert_time_unit(raw_timestamp, timestamp_unit, :nanosecond)
          end
      end

    event_tags = Map.get(metadata, :tags, %{})
    event_metadatas = Map.take(metadata, config.metadata_tag_keys)

    tags =
      Map.merge(config.tags, event_tags)
      |> Map.merge(event_metadatas)

    Formatter.format(event, measurements, tags, event_timestamp)
  end

  def batch_config([{_event, _raw_timestamp, _measurements, _metadata, config} | _rest]),
    do: config
end
