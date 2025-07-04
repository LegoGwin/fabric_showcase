
create   procedure [dbo].[get_task_to_warehouse] @task_id varchar(256) as
begin
    select top 1 * from [dbo].[task_to_warehouse] where [task_id] = @task_id
end;

GO

