defmodule Airbnb do
  def aggr_count_properties(file_path) do
    File.stream!(file_path)
    |> CSV.decode(headers: true, escape_max_lines: 20, mode: :normal)
    |> Enum.map(fn
      {:ok, row} ->{row["neighbourhood_cleansed"], row["property_type"]}
      {:error, _reason} ->
        nil
    end)
    |> Enum.filter(& &1)
    |> Enum.frequencies()
    |> Enum.map(fn {{neighbourhood_cleansed, property_type}, count} ->
      {neighbourhood_cleansed, property_type, count}
    end)
  end
end
