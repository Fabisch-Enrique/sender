# # defmodule Mix.Tasks.Migrations.MigrateBills do
# #   use Mix.Task
# #   alias Pesaflow.Mysql.Repo, as: MysqlRepo
# #   require Logger
# #   use Ecto.Schema
# #   import Ecto.Query, only: [from: 2]

# #   alias Pesaflow.Invoice, as: Invoice
# #   alias Pesaflow.Payment.Gateway

# #   @table "bills"

# #   @shortdoc "Mix task to migrate bills from legacy MySQL platform to new Postgres platform"

# #   def run(args) do
# #     Mix.Task.run("app.start", [])
# #     Logger.info("Task to migrate bills to invoices")

# #     {options, _} = OptionParser.parse!(args, strict: [start: :integer])

# #     window =
# #       Flow.Window.global()
# #       |> Flow.Window.trigger_every(1000)

# #     Stream.resource(
# #       fn -> Keyword.get(options, :start, 0) end,
# #       fn last_id ->
# #         {:ok, records, _} =
# #           Ecto.Migrator.with_repo(MysqlRepo, fn repo ->
# #             from(
# #               i in @table,
# #               where: i.billID > ^last_id,
# #               select: [
# #                 %{
# #                   billID: i.billID,
# #                   client_invoice_ref: i.billRefNumber,
# #                   invoice_number: i.invoiceNumber,
# #                   msisdn: i.clientMSISDN,
# #                   service_id: i.serviceID,
# #                   payment_gateway_id: i.paymentGatewayID,
# #                   merchant_id: i.apiClientID,
# #                   amount_expected: i.amountExpected,
# #                   currency: i.currency,
# #                   amount_paid: i.amountPayed,
# #                   commission: i.commission,
# #                   amount_net: i.net,
# #                   payment_details: i.payerDetails,
# #                   status: i.status,
# #                   confirmed?: i.confirmed,
# #                   inserted_at: i.dateCreated,
# #                   payment_date: i.paymentDate,
# #                   updated_at: i.dateModified,
# #                   notification_url: i.notificationURL
# #                 }
# #               ],
# #               order_by: i.billID,
# #               limit: 2
# #             )
# #             |> repo.all()
# #           end)

# #         records = List.flatten(records)

# #         case records do
# #           [] ->
# #             {:halt, last_id}

# #           records ->
# #             Logger.info("~~~~~~~~~Last id: #{last_id}~~~~~~~~")
# #             last_bill = List.last(records)
# #             {records, last_bill.billID}
# #         end
# #       end,
# #       fn _ -> {:ok, "complete"} end
# #     )
# #     |> Flow.from_enumerable(max_demand: 20)
# #     |> Flow.partition(window: window)
# #     |> Flow.map(&migrate_these_bills/1)
# #     |> Flow.reduce(fn -> 0 end, fn _migrations, accum ->
# #       accum + 1
# #     end)
# #     |> Flow.on_trigger(fn acc -> {[acc], 0} end)
# #     |> Flow.departition(
# #       fn -> 0 end,
# #       fn count, accum ->
# #         accum = accum + count
# #         Logger.info("=======Total processed #{accum}=======")
# #         accum
# #       end,
# #       & &1
# #     )
# #     |> Flow.run()
# #   end

# #   def migrate_these_bills(bill) do
# #     result = convert_into_invoices_map(bill)

# #     Invoice.changeset(%Invoice{}, result)
# #     |> Pesaflow.Repo.insert(on_conflict: :nothing)
# #   end

# #   def convert_into_invoices_map(bill) do
# #     phone_number = bill.msisdn
# #     inserted_at = bill.inserted_at
# #     updated_at = bill.updated_at
# #     confirmed? = bill.confirmed?
# #     payment_date = bill.payment_date

# #     updated =
# #       if not is_nil(updated_at) do
# #         with dt <- DateTime.to_naive(updated_at) do
# #           %NaiveDateTime{dt | microsecond: {elem(dt.microsecond, 0), 6}}
# #         end
# #       end

# #     inserted =
# #       if not is_nil(inserted_at) do
# #         with dt <- inserted_at do
# #           %NaiveDateTime{dt | microsecond: {elem(dt.microsecond, 0), 6}}
# #         end
# #       else
# #         with dt <- DateTime.to_naive(updated_at) do
# #           %NaiveDateTime{dt | microsecond: {elem(dt.microsecond, 0), 6}}
# #         end
# #       end

# #     msisdn = Kernel.inspect(phone_number)

# #     status =
# #       if not is_nil(bill.status) do
# #         cond do
# #           bill.status == 1 -> "successful"
# #           bill.status == 2 -> "pending"
# #           bill.status == 3 -> "cancelled"
# #           bill.status == 4 -> "settled"
# #           bill.status == 5 -> "pending_checker_action"
# #           bill.status == 6 -> "pending_approver_action"
# #           bill.status == 103 -> "escalated_no_bill_found"
# #           bill.status == 666 -> "confirmation_failed"
# #           bill.status == 777 -> "payment_reversed"
# #         end
# #       end

# #     payment_gateway_id =
# #       cond do
# #         bill.payment_gateway_id == 1 -> Gateway.get_by_slug("mpesa-206206")
# #         bill.payment_gateway_id == 2 -> Gateway.get_by_slug("airtel-money-206206")
# #         bill.payment_gateway_id == 3 -> Gateway.get_by_slug("orange-money")
# #         bill.payment_gateway_id == 4 -> Gateway.get_by_slug("yu-cash")
# #         bill.payment_gateway_id == 6 -> Gateway.get_by_slug("mobikash-206206")
# #         bill.payment_gateway_id == 7 -> Gateway.get_by_slug("credit-or-debit-card")
# #         bill.payment_gateway_id == 8 -> Gateway.get_by_slug("equity")
# #         bill.payment_gateway_id == 9 -> Gateway.get_by_slug("pesaflow")
# #         bill.payment_gateway_id == 10 -> Gateway.get_by_slug("cash-kcb")
# #         bill.payment_gateway_id == 11 -> Gateway.get_by_slug("kenswitch")
# #         bill.payment_gateway_id == 12 -> Gateway.get_by_slug("rtgs-to-kcb")
# #         bill.payment_gateway_id == 13 -> Gateway.get_by_slug("pesalink")
# #         bill.payment_gateway_id == 14 -> Gateway.get_by_slug("mpesa-173173")
# #         true -> nil
# #       end

# #     confirmed? =
# #       if not is_nil(confirmed?) do
# #         case confirmed? do
# #           1 -> true
# #           0 -> false
# #         end
# #       end

# #     payment_date =
# #       if not is_nil(payment_date) do
# #         with dt <- payment_date do
# #           {:ok, pd} = DateTime.from_naive(dt, "Etc/UTC")
# #           pd
# #         end
# #       end

# #     payload = %{
# #       bill
# #       | msisdn: msisdn,
# #         inserted_at: inserted,
# #         updated_at: updated,
# #         status: status,
# #         confirmed?: confirmed?,
# #         payment_date: payment_date
# #     }

# #     payload
# #     |> Map.put(:merchant_id, 1)
# #     |> Map.put(:owner_type, "merchant")
# #     |> Map.put(:payment_gateway_id, payment_gateway_id)
# #   end
# # end

# defmodule Ntsa.DlImport do
#   require Logger

#   alias Automzero.User
#   alias Automzero.Repo

#   def import(file_path, datasets) do
#     file_path
#     |> File.stream!(read_ahead: 100_000)
#     |> Flow.from_enumerable()
#     |> Flow.map(&Jason.decode!(&1))
#     |> process(datasets)
#     |> Enum.to_list()
#   end

#   defp process(stream, %{
#          pdl: provisional_dl_dataset,
#          interim: interim_dl_dataset,
#          drvtest: driving_test_dataset,
#          dl: driving_license_dataset
#        }) do
#     output =
#       stream
#       |> Flow.partition()
#       |> Flow.reduce(fn -> [] end, fn entry, acc ->
#         [basic_information] = Map.get(entry, "basicInformation", %{})
#         pdl_listing = Map.get(entry, "pdl", [])
#         test_booking_info_listing = Map.get(entry, "testbookinginfo", [])
#         [interim_dl] = Map.get(entry, "interimDl", %{})
#         [smart_dl] = Map.get(entry, "smartDl", %{})
#         [smart_dl_booking] = Map.get(entry, "smartDlBooking", %{})
#         [foreign_conversion] = Map.get(entry, "foreignConversion", %{})
#         dlclasses = Map.get(entry, "Dlclass", %{})

#         # original pdl data
#         pdl = (length(pdl_listing) > 0 && hd(pdl_listing)) || %{}

#         # original test booking info
#         test_booking_info =
#           (length(test_booking_info_listing) > 0 && hd(test_booking_info_listing)) || %{}

#         pdl_key_map =
#           pdl_listing
#           |> Enum.map(fn pdl ->
#             %{
#               "data" => %{
#                 "drivingSchoolName" => "",
#                 "idNumber" => basic_information["idno"],
#                 "name" => basic_information["givenname"] <> " " <> basic_information["surname"],
#                 "pdlNumber" => pdl["LICENCE_DOCUMENT_N"],
#                 "country" => basic_information["nationality"],
#                 "nationality" => basic_information["nationality"],
#                 # ?? branch lookup
#                 "branch" => pdl["branch"],
#                 "drivingClass" => pdl["DL_LICENCE_CLASS_CD"],
#                 # ?? student regno
#                 "regNo" => pdl["regno"],
#                 "gender" => basic_information["sex"],
#                 "dob" => basic_information["birthday"],
#                 "postalAddress" => basic_information["postaladdress"],
#                 # ?? city lookup
#                 "postalCode" => basic_information["postalCode"],
#                 "physicalAddress" => basic_information["address"],
#                 "mobileNumber" => basic_information["phone"],
#                 "email" => basic_information["email"],
#                 "bloodGroup" => pdl["BLOOD_TYPE_CD"],
#                 "status" => pdl["LICENCE_STATUS_CD"],
#                 "issueDate" => pdl["ISSUE_D"],
#                 "expiryDate" => pdl["EXPIRY_D"],
#                 # ?? date of disqualification
#                 "dateOfDisqualifaction" => pdl["dateOfDisqualifaction"],
#                 "epilepsy" => pdl["EPILEPSY_CD"],
#                 "readDistance" => pdl["READ_DISTANCE_CD"],
#                 "movementControl" => pdl["MOVEMENT_CONTROL_CD"],
#                 "otherDisease" => pdl["OTHER_DECEASE_CD"],
#                 "drivingSchoolKraPin" => pdl["DRIVINGSCHOOLPIN"],
#                 "licenseDocumentStatus" => pdl["LICENCE_DOCUMENT_STATUS_CD"]
#               }
#             }
#           end)

#         interim_dl_key_map = [
#           %{
#             "data" => %{
#               "pdlNumber" => "",
#               "testNumber" => test_booking_info["TESTBOOKINGCONFIRMATIONNUMBER"],
#               "idNumber" => basic_information["idno"],
#               "name" => basic_information["givenname"] <> " " <> basic_information["surname"],
#               "drivingClass" => pdl["DL_LICENCE_CLASS_CD"],
#               "interimNumber" => interim_dl["DOCUMENT_NUMBER"],
#               "nationality" => basic_information["nationality"],
#               "country" => basic_information["nationality"],
#               "gender" => basic_information["sex"],
#               "dob" => basic_information["birthday"],
#               "bloodGroup" => pdl["BLOOD_TYPE_CD"],
#               "physicalAddress" => basic_information["address"],
#               "postalAddress" => basic_information["postaladdress"],
#               "postalCode" => basic_information["postalCode"],
#               "mobileNumber" => basic_information["phone"],
#               "email" => basic_information["email"],
#               "dtuCenter" => test_booking_info["TestingCenterPIN"],
#               "testDocumentStatus" => interim_dl["TEST_DOCUMENT_STATUS_CD"],
#               # ?? Add to interim dataset
#               "issueDate" => interim_dl["ISSUE_D"],
#               "expiryDate" => interim_dl["EXPIRY_D"]
#             }
#           }
#         ]

#         driving_test_key_map =
#           test_booking_info_listing
#           |> Enum.map(fn test_booking_info ->
#             %{
#               "data" => %{
#                 "name" => "#{basic_information["givenname"]} #{basic_information["surname"]}",
#                 "pdl" => "",
#                 "testDate" => test_booking_info["TEST_D"],
#                 # ?? TBD
#                 "resultDate" => test_booking_info["TEST_D"],
#                 "dtuCenter" => test_booking_info["TestingCenterPIN"],
#                 "status" => test_booking_info["TEST_STATUS_CD"],
#                 "testNumber" => test_booking_info["TESTBOOKINGCONFIRMATIONNUMBER"],
#                 # ?? TBD
#                 "drivingClass" => "",
#                 "idNumber" => basic_information["idno"],
#                 "nationality" => basic_information["nationality"],
#                 "gender" => basic_information["gender"],
#                 "dob" => basic_information["birthday"],
#                 "bloodGroup" => pdl["BLOOD_TYPE_CD"],
#                 "physicalAddress" => basic_information["address"],
#                 "postalAddress" => basic_information["postaladdress"],
#                 "postalCode" => basic_information["postalCode"],
#                 "phoneNumber" => basic_information["phone"],
#                 "email" => basic_information["email"],
#                 "country" => basic_information["nationality"],
#                 "testTime" => test_booking_info["TEST_T"],
#                 "drivingSchoolEnrolledAtPin" => test_booking_info["DrivingSchoolEnrolledAtPIN"],
#                 "drivingSchoolTrainedAtPin" => test_booking_info["DrivingSchoolTrainedAtPIN"],
#                 "vehicleControlResults" => test_booking_info["VEHICLE_CONTROL_RESULT_CD"],
#                 "questionsResults" => test_booking_info["QUESTIONS_RESULT_CD"],
#                 "roadProc1Results" => test_booking_info["ROAD_PROC_1_RESULTS_CD"],
#                 "roadProc2Results" => test_booking_info["ROAD_PROC_2_RESULTS_CD"],
#                 "remark" => test_booking_info["REMARK"],
#                 # ?? Add to test booking dataset
#                 "misconductDescription" => test_booking_info["MISCONDUCT_DESC"],
#                 "testDocumentStatus" => test_booking_info["TEST_DOCUMENT_STATUS_CD"]
#               }
#             }
#           end)

#         driving_license_key_map = [
#           %{
#             "data" => %{
#               "full_name" => "#{basic_information["givenname"]} #{basic_information["surname"]}",
#               "national_id" => basic_information["idno"],
#               "interim_number" => interim_dl["DOCUMENT_NUMBER"],
#               "pdl_number" => "",
#               "dlclass" => dlclasses,
#               "blood_group" => pdl["BLOOD_TYPE_CD"],
#               "kra" => basic_information["pin"],
#               # ?? Smart DL Issue Date
#               "date_of_issue" => smart_dl["ISSUE_D"],
#               "date_of_expiry" => smart_dl["EXPIRY_D"],
#               "status" => smart_dl["TEST_DOCUMENT_STATUS_CD"],
#               "license_number" => smart_dl["Document_number"],
#               "date_of_birth" => basic_information["birthday"],
#               "nationality" => basic_information["nationality"],
#               "sex" => basic_information["sex"],
#               "address" => basic_information["address"],
#               "city" => basic_information["city"],
#               "postalAddress" => basic_information["postalAddress"],
#               "phoneNumber" => basic_information["phone"],
#               "email" => basic_information["email"],
#               "smartDlChipId" => smart_dl["CHIP_ID"],
#               "foreignConversionOrganization" => foreign_conversion["ORGANIZATION_NAME"],
#               "foreignConversionExpiryDate" => foreign_conversion["EXPIRY_D"],
#               "foreignConversionIssueDate" => foreign_conversion["ISSUED_DATE"],
#               "foreignConversionDLNumber" => foreign_conversion["DRIVING_LICENCE_N"],
#               "foreignConversionDtlConversionType" =>
#                 foreign_conversion["DTL_CONVERSION_TYPE_CD"],
#               # ?? Add to smart DL to drivers license dataset
#               "smartDLBookingDate" => smart_dl_booking["BOOKING_D"],
#               "smartDLBookingStartDate" => smart_dl_booking["BOOKING_START_D"],
#               "smartDLBookingStatus" => smart_dl_booking["BOOKING_STATUS_CD"],
#               "smartDLBookingTestCenter" => smart_dl_booking["TEST_CENTER_PIN"]
#             }
#           }
#         ]

#         Repo.transaction(fn repo ->
#           #  tims user
#           acc =
#             case create_tims_user(basic_information, repo) do
#               {:ok, _user} -> acc
#               {:error, _} -> [basic_information | acc]
#             end

#           acc =
#             [
#               {provisional_dl_dataset, pdl_key_map},
#               {interim_dl_dataset, interim_dl_key_map},
#               {driving_test_dataset, driving_test_key_map},
#               {driving_license_dataset, driving_license_key_map}
#             ]
#             |> Enum.map(fn {dataset, data} ->
#               if length(data) > 0 do
#                 Logger.info("Inserting records for #{dataset.name}")

#                 dataset
#                 |> dataset.type.create_all(data, %{id: 1}, nil)
#                 |> process(entry, acc)
#               else
#                 acc
#               end
#             end)

#           acc
#         end)

#         acc
#       end)
#       |> Flow.emit(:state)
#       |> Enum.to_list()
#       |> List.flatten()
#       |> Enum.map(&Jason.encode!(&1))

#     output
#   end

#   defp process({:ok, _}, _entry, acc), do: acc
#   defp process({:error, _}, entry, acc), do: [entry | acc]

#   defp create_tims_user(%{"idno" => id_no} = basic_information, repo) do
#     user = repo.get_by(User, id_number: id_no)

#     id_type =
#       cond do
#         id_no |> String.upcase() |> String.starts_with?("A") -> "alien"
#         id_no |> String.upcase() |> String.starts_with?("D") -> "visitor"
#         true -> "citizen"
#       end

#     if user do
#       {:ok, user}
#     else
#       [first_name, last_name] = basic_information["givenname"] |> String.split(" ")

#       %User{}
#       |> User.changeset(%{
#         id_number: basic_information["idno"],
#         first_name: first_name,
#         last_name: last_name,
#         other_name: basic_information["surname"],
#         phone_number: basic_information["phone"],
#         kra_pin: basic_information["pin"],
#         id_type: id_type
#       })
#       |> repo.insert(
#         on_conflict: {:replace_all_except, [:id, :otp_type, :signature]},
#         conflict_target: {:unsafe_fragment, "(lower(id_number), id_type)"},
#         returning: true
#       )
#     end
#   end
# end
