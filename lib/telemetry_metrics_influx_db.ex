defmodule TelemetryMetricsInfluxDB do
  alias TelemetryMetricsInfluxDB.HTTP
  alias TelemetryMetricsInfluxDB.UDP
  require Logger

  @moduledoc """

  """
  @default_port 8086

  @type option ::
          {:port, :inet.port_number()}
          | {:host, String.t()}
          | {:protocol, atom()}
          | {:db, String.t()}
          | {:username, String.t()}
          | {:password, String.t()}
          | {:events, [event]}
          | {:tags, tags}

  @type options :: [option]
  @type event :: %{required(:name) => :telemetry.event_name()}
  @type tags :: map()
  @type event_spec() :: map()
  @type event_name() :: [atom()]
  @type event_measurements :: map()
  @type event_metadata :: map()
  @type handler_config :: term()
  @type handler_id() :: term()

  @spec start_link(options) :: GenServer.on_start()
  def start_link(options) do
    config =
      options
      |> Enum.into(%{})
      |> Map.put_new(:protocol, :udp)
      |> Map.put_new(:host, "localhost")
      |> Map.put_new(:port, @default_port)
      |> Map.put_new(:tags, %{})
      |> validate_required!([:db, :events])
      |> validate_event_fields!()
      |> validate_protocol!()

    start_server(config.protocol, config)
  end

  def start_server(:udp, config) do
    GenServer.start_link(UDP.Connector, config)
  end

  def start_server(:http, config) do
    GenServer.start_link(HTTP.Connector, config)
  end

  def stop(reporter) do
    GenServer.stop(reporter)
  end

  defp validate_protocol!(%{protocol: :udp} = opts), do: opts
  defp validate_protocol!(%{protocol: :http} = opts), do: opts

  defp validate_protocol!(_) do
    raise(ArgumentError, "protocol has to be :udp or :http")
  end

  defp validate_event_fields!(%{events: []}) do
    raise(ArgumentError, "you need to attach to at least one event")
  end

  defp validate_event_fields!(%{events: events} = opts) when is_list(events) do
    Enum.map(events, &validate_required!(&1, :name))
    opts
  end

  defp validate_event_fields!(%{events: _}) do
    raise(ArgumentError, ":events needs to be list of events")
  end

  defp validate_required!(opts, fields) when is_list(fields) do
    Enum.map(fields, &validate_required!(opts, &1))
    opts
  end

  defp validate_required!(opts, field) do
    case Map.has_key?(opts, field) do
      true ->
        opts

      false ->
        raise(ArgumentError, "#{inspect(field)} field needs to be specified")
    end
  end
end
