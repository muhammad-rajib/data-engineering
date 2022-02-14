# write processed data
def to_files(df, store_dir, file_format):
    df.coalesce(16). \
        write. \
        partitionBy('year', 'month', 'day'). \
        mode('append'). \
        format(file_format). \
        save(store_dir)
