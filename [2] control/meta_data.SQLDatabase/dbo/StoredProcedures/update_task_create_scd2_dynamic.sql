create   procedure [dbo].[update_task_create_scd2_dynamic] @task_id varchar(256), @min_date_key varchar(64) as

begin
    update [dbo].[task_create_scd2_dynamic]
    set [min_date_key] = @min_date_key where [task_id] = @task_id

    if @@rowcount = 0
    begin
        insert into [dbo].[task_create_scd2_dynamic] ([task_id], [min_date_key])
        values (@task_id, @min_date_key);
    end
end;

GO

