# defmodule Sender.MigrationBills do
#   use Mix.Task
#   require Logger

#   @table "members"

#   @shortdoc "Mix task to migrate members from docker to members in psql"

#   def run(args) do
#     Mix.Task.run("app.start", [])
#     Logger.info("Task to migrate bills to invoices")

#     {options, _} = OptionParser.parse!(args, strict: [start: :integer])

#     window =
#       Flow.Window.globals()
#       |> Flow.Window.trigger_every(1000)

#     Stream.resource(
#       fn -> Keyword.get(options, :start, 0) end,
#       fn -> last_id -> {:ok, records, _} = Ecto.Migrator.with_repo(MysqlRepo, fn  repo -> from(m in @table, where i.member_id > ^last_id, select: [
#         %{
#           member_id: m.member_id,
#           full_name: m.full_name,
#           registaration_number: m.registaration_number,
#           membership_number: m.membership_number,
#           title: m.title,
#           website: m.website,
#           email: m.email,
#           business_no: m.business_no,
#           county_id: m.county_id,
#           sub_county_id: m.sub_county_id,
#           ward_id: m.ward_id,
#           admission_date: m.admission_date,
#           created_at: m.created_at,
#           deleted_at: m.deleted_at,
#           updated_at: m.updated_at,
#           no_of_employees: m.no_of_employees,
#           consent: m.consent,
#           mse_type: m.mse_type
#         }
#       ], order_by: m.member_id, limit: 2) |> repo.all() end)

#       records = List.flatten(records)

#       case records do
#         [] ->
#           {:halt, last_id}

#         records ->
#           Logger.info("~~~~~~~~~Last id: #{last_id}~~~~~~~~")
#           last_member = List.last(records)
#           {records, last_member.member_id}
#         end
#       end,
#       fn _ -> {:ok, "complete"} end
#     )
#     |> Flow.from_enumerable(max_demand: 20)
#     |> Flow.partition(window: window)
#     |> Flow.map(&migrate_these_members/1)
#     |> Flow.reduce(fn -> 0 end, fn _migrations, accum -> accum + 1 end)
#     |> Flow.on_trigger(fn acc -> {[acc], 0} end)
#     |> Flow.departition(fn -> 0 end, fn count< accum -> accum = accum + count Logger.info("=======Total processsed #{accum}=======")
#     accum end, & &1
#     )
#     |> Flow.run()
#   end

#   def migrate_this_members(members) do
#     result = convert_into_members_map(member)

#     Member.changeset(%Member{}, result)
#     |> Automzero.Repo.insert(on_conflict: :nothing)
#   end

#   def convert_into_members_map(member) do

#   end

# end
