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

  def offer_by_neighbourhood(file_path) do
    File.stream!(file_path)
    |> CSV.decode(headers: true, escape_max_lines: 20, mode: :normal)
    |> Enum.map(fn
      {:ok, row} ->{row["neighbourhood_cleansed"], row["property_type"], row["accommodates"], row["price"]}
      {:error, _reason} ->
        nil
    end)
    |> Enum.filter(fn
      {a, b, c, d} -> not Enum.any?([a, b, c, d], fn x -> x in [nil, ""] end)
      _ -> false  # si no es una tupla, lo descartamos
    end)
    |> Enum.group_by(fn {neighbourhood, property_type, _acc, _price} ->
      {neighbourhood, property_type}
    end)
    |> Enum.map(fn {{neigh, _prop_type}, listings} ->
      total_price =
        listings
        |> Enum.map(fn {_n, _p, _acc, price_str} ->
          price_str
          |> String.replace("$", "")
          |> String.replace(",", "")
          |> String.to_float()
        end)
        |> Enum.sum()

      total_accommodates =
        listings
        |> Enum.map(fn {_n, _p, acc_str, _price_str} ->
          acc_str |> String.to_integer()
        end)
        |> Enum.sum()

      average_price_per_person = if total_accommodates > 0, do: total_price / total_accommodates, else: 0.0
      _listing_data =
        Enum.map(listings, fn {_n, _p, acc_str, price_str} ->
          acc = acc_str |> String.to_integer()
          price =
            price_str
            |> String.replace("$", "")
            |> String.replace(",", "")
            |> String.to_float()

          {acc, price}
        end)
      {neigh, total_accommodates, average_price_per_person}

    end)
  end
end
