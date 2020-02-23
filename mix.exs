defmodule Kvasir.Postgres.MixProject do
  use Mix.Project

  @version "1.0.0"

  def project do
    [
      app: :kvasir_postgres,
      version: "0.0.1",
      description: "PostgreSQL based cold storage for Kvasir.",
      version: @version,
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),

      # Testing
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ],
      dialyzer: [ignore_warnings: ".dialyzer", plt_add_deps: true],

      # Docs
      name: "Kvasir PostgreSQL",
      source_url: "https://github.com/IanLuites/kvasir_postgres",
      homepage_url: "https://github.com/IanLuites/kvasir_postgres",
      docs: [
        main: "readme",
        extras: ["README.md"],
        source_ref: "v#{@version}",
        source_url: "https://github.com/IanLuites/kvasir_postgres"
      ]
    ]
  end

  def package do
    [
      name: :kvasir_postgres,
      maintainers: ["Ian Luites"],
      licenses: ["MIT"],
      files: [
        # Elixir
        "lib/kvasir",
        ".formatter.exs",
        "mix.exs",
        "README*",
        "LICENSE*"
      ],
      links: %{
        "GitHub" => "https://github.com/IanLuites/kvasir_postgres"
      }
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:kvasir, git: "https://github.com/IanLuites/kvasir", branch: "release/v1.0"},
      {:postgrex, "~> 0.15.1"},

      # Dev / Test
      {:analyze, "~> 0.1", only: [:dev, :test], runtime: false, override: true},
      {:dialyxir, "~> 1.0.0-rc.7", only: [:dev], runtime: false}
    ]
  end
end
