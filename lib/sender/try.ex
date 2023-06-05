# defmodule Mix.Tasks.Migrations.MigrateBills do
#   use Mix.Task
#   alias Pesaflow.Mysql.Repo, as: MysqlRepo
#   require Logger
#   use Ecto.Schema
#   import Ecto.Query, only: [from: 2]

#   alias Pesaflow.Invoice, as: Invoice
#   alias Pesaflow.Payment.Gateway

#   @table "bills"

#   @shortdoc "Mix task to migrate bills from legacy MySQL platform to new Postgres platform"

#   def run(args) do
#     Mix.Task.run("app.start", [])
#     Logger.info("Task to migrate bills to invoices")

#     {options, _} = OptionParser.parse!(args, strict: [start: :integer])

#     window =
#       Flow.Window.global()
#       |> Flow.Window.trigger_every(1000)

#     Stream.resource(
#       fn -> Keyword.get(options, :start, 0) end,
#       fn last_id ->
#         {:ok, records, _} =
#           Ecto.Migrator.with_repo(MysqlRepo, fn repo ->
#             from(
#               i in @table,
#               where: i.billID > ^last_id,
#               select: [
#                 %{
#                   billID: i.billID,
#                   client_invoice_ref: i.billRefNumber,
#                   invoice_number: i.invoiceNumber,
#                   msisdn: i.clientMSISDN,
#                   service_id: i.serviceID,
#                   payment_gateway_id: i.paymentGatewayID,
#                   merchant_id: i.apiClientID,
#                   amount_expected: i.amountExpected,
#                   currency: i.currency,
#                   amount_paid: i.amountPayed,
#                   commission: i.commission,
#                   amount_net: i.net,
#                   payment_details: i.payerDetails,
#                   status: i.status,
#                   confirmed?: i.confirmed,
#                   inserted_at: i.dateCreated,
#                   payment_date: i.paymentDate,
#                   updated_at: i.dateModified,
#                   notification_url: i.notificationURL
#                 }
#               ],
#               order_by: i.billID,
#               limit: 2
#             )
#             |> repo.all()
#           end)

#         records = List.flatten(records)

#         case records do
#           [] ->
#             {:halt, last_id}

#           records ->
#             Logger.info("~~~~~~~~~Last id: #{last_id}~~~~~~~~")
#             last_bill = List.last(records)
#             {records, last_bill.billID}
#         end
#       end,
#       fn _ -> {:ok, "complete"} end
#     )
#     |> Flow.from_enumerable(max_demand: 20)
#     |> Flow.partition(window: window)
#     |> Flow.map(&migrate_these_bills/1)
#     |> Flow.reduce(fn -> 0 end, fn _migrations, accum ->
#       accum + 1
#     end)
#     |> Flow.on_trigger(fn acc -> {[acc], 0} end)
#     |> Flow.departition(
#       fn -> 0 end,
#       fn count, accum ->
#         accum = accum + count
#         Logger.info("=======Total processed #{accum}=======")
#         accum
#       end,
#       & &1
#     )
#     |> Flow.run()
#   end

#   def migrate_these_bills(bill) do
#     result = convert_into_invoices_map(bill)

#     Invoice.changeset(%Invoice{}, result)
#     |> Pesaflow.Repo.insert(on_conflict: :nothing)
#   end

#   def convert_into_invoices_map(bill) do
#     phone_number = bill.msisdn
#     inserted_at = bill.inserted_at
#     updated_at = bill.updated_at
#     confirmed? = bill.confirmed?
#     payment_date = bill.payment_date

#     updated =
#       if not is_nil(updated_at) do
#         with dt <- DateTime.to_naive(updated_at) do
#           %NaiveDateTime{dt | microsecond: {elem(dt.microsecond, 0), 6}}
#         end
#       end

#     inserted =
#       if not is_nil(inserted_at) do
#         with dt <- inserted_at do
#           %NaiveDateTime{dt | microsecond: {elem(dt.microsecond, 0), 6}}
#         end
#       else
#         with dt <- DateTime.to_naive(updated_at) do
#           %NaiveDateTime{dt | microsecond: {elem(dt.microsecond, 0), 6}}
#         end
#       end

#     msisdn = Kernel.inspect(phone_number)

#     status =
#       if not is_nil(bill.status) do
#         cond do
#           bill.status == 1 -> "successful"
#           bill.status == 2 -> "pending"

