defmodule Migration2 do
  require Logger

  NimbleCSV.define(MyParser, separator: "\t", escape: "\"")

  def migrate_data(file_path) do

    Logger.info("Migrating members, associations and officials")

    [headers | data] =
      File.stream!(file_path, read_ahead: 100_000)
      |> MyParser.parse_stream(skip_headers: false)
      |> Enum.to_list()

    new_headers = %{
      "gender" => "gender",
      "full_name" => "full_name",
      "membership_number" => "membership_number",
      "admission_date" => "admission_date",
      "title" => "title",
      "email" => "email",
      "website" => "website",
      "county_id" => "county_id",
      "sub_county_id" => "sub_county_id",
      "ward_id" => "ward_id",
      "reg_created_at" => "creation_date",
      "reg_updated_at" => "updated_date",
      "reg_deleted_at" => "deletion_date",
      "actual_no_of_employees" => "no_of_employees",
      "consent" => "consent",
      "mse_type" => "mse_type"
    }

    transformed_data =
      data
      |> Enum.map(&Enum.zip(headers, &1))
      |> Enum.map(fn row ->
        Enum.reduce(row, %{}, fn {old_header, value}, acc ->
          new_header = Map.get(new_headers, old_header)
          if !is_nil(new_header) do
            Map.put(acc, new_header, value)
          else
            acc
          end
        end)
      end)


      Task.async_stream(transformed_data, &insert_all_into_new_table/1)
        |> Task.yield_many(:infinity)
  end

  defp insert_all_into_new_table(row) do
    changeset = Sender.changeset(%Member{}, row)
    case Repo.insert(changeset) do
      {:ok, _user} ->
        Logger.info("Row inserted successfully")
      {:error, changeset} ->
        Logger.error("Failed to insert row: #{inspect(changeset.errors)}")
    end
   end
end
