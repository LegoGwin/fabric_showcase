create   procedure [dbo].[get_control_task_batch] @job_id int, @batch_num int as

begin

    declare @task_sql nvarchar(1024);
    set @task_sql = (select top 1 [task_sql] from [dbo].[control_jobs] where [job_id] = @job_id);

    create table #task_ids ([task_id] varchar(256));
    insert into #task_ids ([task_id]) exec sp_executesql @task_sql;

    with [filtered] as
    (
        select
            [tasks].[task_id],
            [ctt].[dataset_path]
        from
            [dbo].[control_tasks] as [tasks]
            left join [dbo].[control_task_target] as [ctt]
                on [tasks].[task_id] = [ctt].[task_id]
        where
            [tasks].[task_id] in (select * from #task_ids)
    ),
    [tasks] as
    (
        select
            [tasks].[task_id],
            [parents].[task_id] as [parent_id]
        from
            [filtered] as [tasks]
            left join [dbo].[dataset_lineage] as [ds]
                on [tasks].[dataset_path] = [ds].[dataset_path]
            left join [filtered] as [parents]
                on [ds].[parent_path] = [parents].[dataset_path]
    ),
    [batches] as
    (
        select
            [tasks].[task_id],
            1 as [batch_num]
        from
            [tasks]
        where 
            [tasks].[parent_id] is null

        union all

        select
            [tasks].[task_id],
            [batches].[batch_num] + 1
        from
            [tasks]
            inner join [batches]
                on [tasks].[parent_id] = [batches].[task_id]
    ),
    [max_batch] as
    (
        select
            [batches].[task_id],
            max([batches].[batch_num]) as [batch_num]
        from
            [batches]
        group by
            [batches].[task_id]
    )

    select
        [max_batch].[task_id],
        left([max_batch].[task_id], charindex(':', [max_batch].[task_id]) - 1) as [pipeline_id],
        [max_batch].[batch_num]
    from
        [max_batch]
    where
        [max_batch].[batch_num] = @batch_num

end

GO

