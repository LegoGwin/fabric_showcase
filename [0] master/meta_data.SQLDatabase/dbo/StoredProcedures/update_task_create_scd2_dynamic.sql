create   procedure [dbo].[update_task_create_scd2_dynamic] @task_id varchar(256), @min_partition varchar(64) as

begin
    update [dbo].[task_create_scd2_dynamic]
    set [min_partition] = @min_partition where [task_id] = @task_id

    if @@rowcount = 0
    begin
        insert into [dbo].[task_create_scd2_dynamic] ([task_id], [min_partition])
        values (@task_id, @min_partition);
    end
end;

GO

