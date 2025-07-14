
create   procedure [dbo].[get_task_create_scd2] @task_id varchar(256) as
begin
    select top 1 * from [dbo].[task_create_scd2] where [task_id] = @task_id
end;

GO

