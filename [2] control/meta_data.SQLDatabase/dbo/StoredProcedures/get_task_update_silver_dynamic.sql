create   procedure [dbo].[get_task_update_silver_dynamic] @task_id varchar(256) as

begin
    select top 1
        *
    from
        [dbo].[task_update_silver_dynamic]
    where
        [task_id] = @task_id
end;

GO

