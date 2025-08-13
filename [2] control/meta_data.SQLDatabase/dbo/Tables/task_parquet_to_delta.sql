CREATE TABLE [dbo].[task_parquet_to_delta] (
    [task_id]        VARCHAR (256) NULL,
    [target_path]    VARCHAR (256) NULL,
    [source_path]    VARCHAR (256) NULL,
    [partition_name] VARCHAR (64)  NULL,
    [full_refresh]   VARCHAR (8)   NULL
);


GO

