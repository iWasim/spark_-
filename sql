DECLARE @sql NVARCHAR(MAX) = '';

SELECT @sql += 'INSERT INTO dbo.' + QUOTENAME(table_name) + ' SELECT * FROM stg.' + QUOTENAME(table_name) + '; '
FROM information_schema.tables
WHERE table_name LIKE 'tablename%';  -- Adjust 'tablename%' as needed

-- Execute the constructed SQL
EXEC(@sql);

