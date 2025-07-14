
create procedure [dbo].[get_task_json_to_delta_flatten] @task_id varchar(256) as
begin
    select * from [dbo].[task_json_to_delta_flatten] where [task_id] = @task_id
end;

GO

