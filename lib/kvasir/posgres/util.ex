defmodule Kvasir.Postgres.Util do
  @moduledoc false
  require Logger

  ### Settings ###

  @defaults [
    username: "postgres",
    password: "postgres",
    hostname: "localhost",
    database: "postgres"
  ]

  @doc ~S"""
  Configure a PostgreSQL connection.

  ## Examples

  Defaults to standard PostgreSQL settings:
  ```elixir
  iex> settings()
  [username: "postgres", password: "postgres", hostname: "localhost", database: "postgres"]
  ```

  ```elixir
  iex> settings(hostname: "example.com", username: nil)
  [password: "postgres", database: "postgres", hostname: "example.com"]
  ```

  ```elixir
  iex> settings(url: "postgres://my-user:my-pass@my-host/my-db")
  [username: "my-user", password: "my-pass", hostname: "my-host", database: "my-db"]
  ```
  """
  @spec settings(Keyword.t()) :: Keyword.t()
  def settings(opts \\ [])

  def settings(opts) do
    {u, settings} = Keyword.pop(opts, :url)

    @defaults
    |> Keyword.merge(url(u))
    |> Keyword.merge(settings)
    |> Enum.reject(&(elem(&1, 1) == nil))
  end

  @doc ~S"""
  Configure PostgreSQL based on a URI.

  ## Examples

  No URI, no settings:
  ```elixir
  iex> url(nil)
  []
  ```

  Basic configuration:
  ```elixir
  iex> url("postgres://my-host/my-db")
  [hostname: "my-host", database: "my-db"]
  ```

  Allows settings username and/or password:
  ```elixir
  iex> url("postgres://my-user:@my-host/my-db")
  [username: "my-user", hostname: "my-host", database: "my-db"]

  iex> url("postgres://:my-pass@my-host/my-db")
  [password: "my-pass", hostname: "my-host", database: "my-db"]

  iex> url("postgres://my-user:my-pass@my-host/my-db")
  [username: "my-user", password: "my-pass", hostname: "my-host", database: "my-db"]
  ```

  Allows for custom port:
  ```elixir
  iex> url("postgres://my-host:8888/my-db")
  [hostname: "my-host", database: "my-db", port: 8888]
  ```

  Scheme must be `postgres`:
  ```
  iex> url("https://example.com/my-db")
  []
  ```
  """
  @spec url(String.t() | nil) :: Keyword.t()
  def url(uri)
  def url(nil), do: []

  def url(uri) do
    case URI.parse(uri) do
      uri = %URI{scheme: "postgres"} ->
        {user, pass} =
          case :binary.split(uri.userinfo || "", ":") do
            [""] -> {nil, nil}
            [a, b] -> {if(a != "", do: a), if(b != "", do: b)}
          end

        Enum.reject(
          [
            username: user,
            password: pass,
            hostname: uri.host,
            database: uri.path && String.trim(uri.path, "/"),
            port: uri.port
          ],
          &(elem(&1, 1) == nil)
        )

      _ ->
        Logger.error(fn -> "#{inspect(__MODULE__)}: Invalid PostgreSQL URI set." end)
        []
    end
  end
end
