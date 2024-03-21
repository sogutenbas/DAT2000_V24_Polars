import polars as pl

file_path="/Users/serhatberg/Desktop/Polars/kjoretoyinfo_fra_2020.parquet"
# question 1
df=pl.scan_parquet(file_path)

# question 2
df = df.with_columns([
    pl.col('tekn_reg_f_g_n').cast(pl.Utf8).str.strptime(pl.Date, '%Y%m%d', strict=False),
    pl.col('tekn_reg_eier_dato').cast(pl.Utf8).str.strptime(pl.Date, '%Y%m%d', strict=False),
    pl.col('tekn_neste_pkk').cast(pl.Utf8).str.strptime(pl.Date, '%Y%m%d', strict=False),
])
print(df.collect())

# question 3
fargekode_df = pl.read_csv("/Users/serhatberg/Desktop/Polars/fargekode.csv", separator=";", truncate_ragged_lines=True).with_columns(pl.col("kode").cast(pl.Utf8))

fargekode_df_lazy = fargekode_df.lazy()

joined_df = df.join(
    fargekode_df_lazy,
    left_on='tekn_farge',
    right_on='kode',
    how='left'
)
print(joined_df.collect())

# question 4
drivstoffkoder_df = pl.read_csv('/Users/serhatberg/Desktop/Polars/drivstoffkoder.csv', separator=";", truncate_ragged_lines=True).with_columns(pl.col("kode").cast(pl.Utf8))
drivstoffkoder_df_lazy = drivstoffkoder_df.lazy()

joined_df = joined_df.join(
    drivstoffkoder_df_lazy,
    left_on='tekn_drivstoff',
    right_on='kode',
    how='left'
)
joined_df = joined_df.with_columns(
    (pl.col('navn') == "Elektrisk").alias('elbil')
)
print(joined_df.collect())

# question 5
selected_columns = joined_df.select([
    'tekn_reg_f_g_n', 'tekn_reg_eier_dato', 'tekn_aksler_drift', 'tekn_merke', 'tekn_modell', 'tekn_drivstoff', 'tekn_neste_pkk', 'beskrivelse', 'elbil', "tekn_kjtgrp"
])
print(selected_columns.collect())

# question 6
selected_columns.collect().write_parquet('kjoretoyinfo_preppet.parquet')

# read file
df_final = pl.scan_parquet('/Users/serhatberg/Desktop/Polars/kjoretoyinfo_preppet.parquet')

# question 7
print(df_final.filter(pl.col("elbil") == True).filter(pl.col("tekn_reg_f_g_n").dt.year() == 2022).collect().count())

# question 8
sold2022Count = df_final.filter(pl.col("tekn_reg_eier_dato").dt.year() == 2022).collect().count()
sold2022CountWithElbil = df_final.filter(pl.col("elbil") == True).filter(pl.col("tekn_reg_eier_dato").dt.year() == 2022).collect().count()
percentage = sold2022CountWithElbil * 100 / sold2022Count

# question 9
df_2022 = df_final.filter(pl.col("tekn_reg_eier_dato").dt.year() == 2022)
most_popular_model = df_2022.group_by("tekn_merke").agg(pl.len().alias("count")).sort("count", descending=True).collect().head(1)
most_popular_model

# question 10
print(df_final.filter(pl.col("beskrivelse") == "Gull (også bronse, gull metallic)").filter(pl.col("tekn_reg_eier_dato").dt.year() == 2022).filter(pl.col("tekn_reg_eier_dato").dt.month() == 5).collect().count())

# question 11
print(df_final.filter(pl.col("tekn_aksler_drift") == 2).collect().count() * 100 / df_final.collect().count())

# question 12
print(df_final.group_by("tekn_reg_f_g_n").agg(pl.len().alias("count")).sort("count", descending=True).collect().head(1).select("tekn_reg_f_g_n"))

# question 13
df_yearly_color = df_final.with_columns(pl.col("tekn_reg_f_g_n").dt.year().alias("year"))

df_popular_color = (df_yearly_color
                    .filter(pl.col("beskrivelse").is_not_null())
                    .group_by(["year", "beskrivelse"])
                    .agg(pl.len().alias("count"))
                    .sort(["year", "count"], descending=True))

df_most_popular_color_each_year = (df_popular_color
                                   .group_by("year")
                                   .agg(pl.col("beskrivelse").first().alias("Most Popular Color"),
                                        pl.col("count").first().alias("Count")))

print(df_most_popular_color_each_year.collect())

# question 14
print(df_final.filter(pl.col("tekn_kjtgrp") == 401)
                .filter(pl.col("tekn_reg_f_g_n").dt.year() == 2022)
                .filter(pl.col("beskrivelse").is_not_null())
                .group_by("beskrivelse")
                .agg(pl.len().alias("count"))
                .sort("count", descending=True)
                .head(1).collect())

# question 15
print(df_final.filter(pl.col("tekn_reg_eier_dato").dt.year() == 2022).group_by("tekn_merke").agg(pl.len().alias("count")).sort("count", descending=True).collect().head(5))

# question 16
df_2022 = df_final.filter(pl.col("tekn_reg_eier_dato").dt.year() == 2022)

top_5_brands = (df_2022.group_by("tekn_merke")
                .agg(pl.len().alias("count"))
                .sort("count", descending=True)
                .head(5)
                .collect())  

top_5_brands_list = top_5_brands["tekn_merke"].to_list()

most_popular_colors_for_top_5_brands = (
    df_2022.filter(pl.col("tekn_merke").is_in(top_5_brands_list))
    .group_by(["tekn_merke", "beskrivelse"])
    .agg(pl.len().alias("count"))
    .sort(["tekn_merke", "count"], descending=[True, True])
)

most_popular_color_per_brand = (most_popular_colors_for_top_5_brands
                                .filter(pl.col("beskrivelse").is_not_null())
                                .group_by("tekn_merke")
                                .agg([
                                    pl.col("beskrivelse").first().alias("Most Popular Color"),
                                    pl.col("count").first()
                                ])
                                )

print(most_popular_color_per_brand.collect())
