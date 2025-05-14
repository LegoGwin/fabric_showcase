CREATE TABLE [dbo].[task_update_silver] (
    [task_id]          VARCHAR (256) NULL,
    [target_path]      VARCHAR (256) NULL,
    [source_path]      VARCHAR (256) NULL,
    [write_method]     VARCHAR (16)  NULL,
    [partition_update] VARCHAR (16)  NULL,
    [full_refresh]     VARCHAR (16)  NULL
);


GO

