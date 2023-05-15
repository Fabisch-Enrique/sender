defmodule Sender.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      %{
        id: Sender.EmailTaskSupervisor,
        start: {
          Task.Supervisor,
          :start_link,
          [[name: Sender.EmailTaskSupervisor]]
        }
      }
      # {Task.Supervisor, name: Sender.EmailTaskSupervisor}
      # Starts a worker by calling: Sender.Worker.start_link(arg)
      # {Sender.Worker, arg}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Sender.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
