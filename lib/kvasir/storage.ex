defmodule Kvasir.Storage.Postgres do
  @moduledoc ~S"""
  PostgreSQL Kvasir cold storage.
  """
  alias Kvasir.Offset
  @big_int_offset 9_223_372_036_854_775_808
  @behaviour Kvasir.Storage
  require Logger

  @impl Kvasir.Storage
  def offsets(name, topic) do
    offset = Enum.reduce(0..(topic.partitions - 1), Offset.create(), &Offset.set(&2, &1, 0))

    with {:ok, %Postgrex.Result{rows: rows}} <-
           Postgrex.query(
             name,
             """
             SELECT partition, max_offset
             FROM topics
             WHERE topic=$1;
             """,
             [topic.topic]
           ) do
      {:ok,
       Enum.reduce(rows, offset, fn [p, o], acc -> Offset.set(acc, p, o + @big_int_offset) end)}
    end
  end

  @impl Kvasir.Storage
  def contains?(_name, _topic, nil), do: :maybe

  def contains?(name, topic, offset) do
    case Postgrex.query(
           name,
           """
           SELECT partition, min_offset, max_offset
           FROM topics
           WHERE topic=$1 AND (#{
             2..(map_size(offset.partitions) + 1)
             |> Enum.map(&"partition = $#{&1}")
             |> Enum.join(" OR ")
           });
           """,
           [topic.topic | Map.keys(offset.partitions)]
         ) do
      {:ok, %Postgrex.Result{rows: rows}} -> contains_reduce(rows, offset.partitions)
      _ -> :maybe
    end
  end

  @spec contains_reduce([list], map, true | false | :maybe | nil) :: true | false | :maybe
  defp contains_reduce(partitions, offsets, acc \\ nil)

  defp contains_reduce(_, _, :maybe), do: :maybe
  defp contains_reduce([], _, nil), do: false
  defp contains_reduce([], _, acc), do: acc

  defp contains_reduce([[p, min, max] | ps], offsets, acc) do
    off = offsets[p] - @big_int_offset

    m =
      cond do
        max < off -> false
        min > off -> false
        min <= off and off <= max -> true
      end

    cond do
      is_nil(acc) -> contains_reduce(ps, offsets, m)
      acc == m -> contains_reduce(ps, offsets, m)
      acc != m -> :maybe
    end
  end

  @impl Kvasir.Storage
  def freeze(pg, topic, event)

  def freeze(
        pg,
        %{topic: topic, module: m, key: key},
        event = %t{__meta__: %{key: k, partition: partition, offset: offset, timestamp: ts}}
      ) do
    {:ok, payload} = m.bin_encode(event)
    {:ok, id} = id(k, key)

    Postgrex.query!(
      pg,
      """
      INSERT INTO "topic_#{topic}"
      (partition, p_offset, id, type, event, committed)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT DO NOTHING;
      """,
      [
        partition,
        offset - @big_int_offset,
        id,
        t.__event__(:type),
        payload,
        UTCDateTime.to_naive(ts)
      ]
    )

    Postgrex.query!(
      pg,
      """
      INSERT INTO topics (topic, partition, min_offset, max_offset)
      VALUES ($1, $2, $3, $3)
      ON CONFLICT (topic, partition)
      DO UPDATE
      SET max_offset=$3
      WHERE topics.topic=$1 AND topics.partition=$2 AND topics.max_offset < $3
      """,
      [
        topic,
        partition,
        offset - @big_int_offset
      ]
    )

    :ok
  end

  @impl Kvasir.Storage
  def stream(pg, topic, opts \\ [])

  def stream(pg, topic = %{module: decoder, key: key, partitions: partitions}, opts) do
    events = events(opts[:events])
    k = opts[:key] || opts[:id]
    partition = Keyword.get(opts, :partition)

    id =
      if k do
        {:ok, i} = id(k, key)
        i
      end

    partition = if(k, do: elem(key.partition(k, partitions), 1), else: partition)

    {q, v} = query(events, id, partition, opts[:from])

    r =
      Enum.map(
        Postgrex.query!(
          pg,
          """
          SELECT partition, p_offset, id, event
          FROM "topic_#{topic.topic}" #{q}
          ORDER BY committed ASC;
          """,
          v
        ).rows,
        fn [a, b, c, d] ->
          with {:ok, event = %{__meta__: m}} <- decoder.bin_decode(d),
               {:ok, ck} <- Jason.decode(c),
               {:ok, pk} <- key.parse(ck, []) do
            {:ok,
             %{
               event
               | __meta__: %{
                   m
                   | key: pk,
                     key_type: key,
                     topic: topic.topic,
                     partition: a,
                     offset: b + @big_int_offset
                 }
             }}
          end
          |> elem(1)
        end
      )

    Logger.debug(fn -> "#{inspect(__MODULE__)}[#{inspect(pg)}]: Read #{Enum.count(r)}" end)

    {:ok, r}
  end

  defp events(nil), do: nil
  defp events(events) when is_list(events), do: Enum.map(events, & &1.__event__(:type))
  defp events(event) when is_atom(event), do: [event.__event__(:type)]
  defp events(_), do: nil

  defp query(type, id, partition, offset)
  defp query(nil, nil, nil, nil), do: {"", []}

  defp query([event], nil, nil, nil), do: {"WHERE type LIKE $1", [event]}
  defp query(events, nil, nil, nil) when is_list(events), do: {"WHERE type in $1", [events]}
  defp query(nil, id, nil, nil), do: {"WHERE id = $1", [id]}
  defp query(nil, id, partition, nil), do: {"WHERE partition = $1 AND id = $2", [partition, id]}

  defp query(_type, nil, _partition, offset = %{}) do
    q =
      0..(map_size(offset.partitions) - 1)
      |> Enum.map(fn i -> "(partition = $#{i * 2 + 1} AND p_offset >= $#{i * 2 + 2})" end)
      |> Enum.join(" OR ")

    {"WHERE #{q}", Enum.flat_map(offset.partitions, fn {k, v} -> [k, v - @big_int_offset] end)}
  end

  defp query(nil, id, partition, offset),
    do:
      {"WHERE partition = $1 AND id = $2 AND p_offset >= $3",
       [partition, id, (offset.partitions[partition] || 0) - @big_int_offset]}

  defp query(_, _, _, nil), do: {"", []}

  defp query(_type, nil, _partition, offset) do
    q =
      0..(map_size(offset.partitions) - 1)
      |> Enum.map(fn i -> "(partition = $#{i * 2 + 1} AND p_offset >= $#{i * 2 + 2})" end)
      |> Enum.join(" OR ")

    {"WHERE #{q}", Enum.flat_map(offset.partitions, fn {k, v} -> [k, v - @big_int_offset] end)}
    # q2 =
    #   (map_size(offset.partitions) * 2 + 1)..(map_size(offset.partitions) * 3)
    #   |> Enum.map(fn i -> "partition != $#{i}" end)
    #   |> Enum.join(" AND ")

    # {"WHERE #{q} OR (#{q2})",
    #  Enum.flat_map(offset.partitions, fn {k, v} -> [k, v] end) ++ Map.keys(offset.partitions)}
  end

  # defp query(type, nil, nil, nil), do: {"WHERE type = $1", [type]}
  # defp query(nil, id, nil, nil), do: {"WHERE id = $1", [id]}
  # defp query(nil, nil, partition, nil), do: {"WHERE partition = $1", [partition]}

  # defp query(nil, nil, partition, offset),
  #   do: {"WHERE partition = $1 AND p_offset >= $2", [partition, offset]}

  # defp query(type, id, nil, nil), do: {"WHERE type = $1 AND id = $2", [type, id]}

  # defp query(nil, id, partition, nil), do: {"WHERE id = $1 AND partition = $2", [id, partition]}

  # defp query(type, nil, partition, nil),
  #   do: {"WHERE type = $1 AND partition = $2", [type, partition]}

  # defp query(type, id, partition, nil),
  #   do: {"WHERE type = $1 AND id = $2 AND partition = $3", [type, id, partition]}

  # defp query(nil, id, partition, offset),
  #   do: {"WHERE id = $1 AND partition = $2 AND p_offset >= $3", [id, partition, offset]}

  # defp query(type, nil, partition, offset),
  #   do: {"WHERE type = $1 AND partition = $2 AND p_offset >= $3", [type, partition, offset]}

  # defp query(type, id, partition, offset),
  #   do:
  #     {"WHERE type = $1 AND id = $2 AND partition = $3 AND p_offset >= $4",
  #      [type, id, partition, offset]}

  defp initialize(pg, topics) do
    # # For testing
    # Enum.each(Map.keys(topics), fn topic ->
    #   Postgrex.query!(
    #     pg,
    #     """
    #     DROP TABLE IF EXISTS "topic_#{topic}";
    #     """,
    #     []
    #   )
    # end)

    # Postgrex.query!(
    #   pg,
    #   """
    #   DROP TABLE IF EXISTS topics;
    #   """,
    #   []
    # )

    Postgrex.query!(
      pg,
      """
      CREATE TABLE IF NOT EXISTS topics
      (
        topic TEXT NOT NULL,
        partition INTEGER NOT NULL,
        min_offset BIGINT NOT NULL,
        max_offset BIGINT NOT NULL,
        PRIMARY KEY (topic, partition)
      );
      """,
      []
    )

    Enum.each(topics, fn {topic, partitions} -> initialize_topic(pg, topic, partitions) end)
  end

  def initialize_topic(pg, topic, partitions) do
    Postgrex.query!(
      pg,
      """
      CREATE TABLE IF NOT EXISTS "topic_#{topic}"
      (
        partition INTEGER NOT NULL,
        p_offset BIGINT NOT NULL,
        id TEXT NOT NULL,
        type TEXT NOT NULL,
        event BYTEA NOT NULL,
        committed TIMESTAMP NOT NULL,
        PRIMARY KEY (partition, p_offset)
      ) PARTITION BY HASH(partition);
      """,
      []
    )

    Enum.each(0..(partitions - 1), fn p ->
      Postgrex.query!(
        pg,
        """
        CREATE TABLE IF NOT EXISTS  "topic_#{topic}_p#{p}" PARTITION OF "topic_#{topic}" FOR VALUES WITH (MODULUS #{
          partitions
        }, REMAINDER #{p});
        """,
        []
      )
    end)

    Postgrex.query!(
      pg,
      """
      CREATE INDEX IF NOT EXISTS "index_topic_#{topic}_type" ON "topic_#{topic}"(type);
      """,
      []
    )

    Postgrex.query!(
      pg,
      """
      CREATE INDEX IF NOT EXISTS "index_topic_#{topic}_id" ON "topic_#{topic}"(id);
      """,
      []
    )

    Postgrex.query!(
      pg,
      """
      CREATE INDEX IF NOT EXISTS "index_topic_#{topic}_partition" ON "topic_#{topic}"(partition);
      """,
      []
    )
  end

  ### Connection ###

  import Kvasir.Postgres.Util, only: [settings: 1]

  @impl Kvasir.Storage
  def child_spec(name, opts \\ []) do
    %{
      id: name,
      start: {__MODULE__, :start_link, [name, opts]}
    }
  end

  @doc false
  @spec start_link(atom, Keyword.t()) :: {:ok, pid} | {:error, term}
  def start_link(name, opts) do
    init = opts[:initialize] || []
    max_pool = opts[:max_pool] || 15
    pool_size = init |> Enum.map(&elem(&1, 1)) |> Enum.sum() |> min(max_pool)

    settings = [
      {:name, name},
      {:pool_size, pool_size},
      {:queue_target, opts[:queue_target] || 100},
      {:queue_interval, opts[:queue_interval] || 1_000}
      | settings(opts)
    ]

    with {:ok, pid} <- Postgrex.start_link(settings) do
      initialize(pid, init)
      {:ok, pid}
    end
  end

  ### Helpers ###

  def id(key, type) do
    with {:ok, d} <- type.dump(key), do: Jason.encode(d)
  end
end
