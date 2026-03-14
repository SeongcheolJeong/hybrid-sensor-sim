import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
} from "@tanstack/react-table";

export { createColumnHelper, type ColumnDef };

export function DataTable<TData>({ data, columns }: { data: TData[]; columns: ColumnDef<TData, any>[] }) {
  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  return (
    <div className="overflow-hidden rounded-2xl border border-cp-border">
      <table className="min-w-full divide-y divide-cp-border text-left text-sm text-cp-text">
        <thead className="bg-cp-surface/80 text-xs uppercase tracking-[0.16em] text-cp-text-muted">
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <th key={header.id} className="px-4 py-3 font-medium">
                  {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody className="divide-y divide-cp-border/70 bg-cp-panel/60">
          {table.getRowModel().rows.map((row) => (
            <tr key={row.id} className="hover:bg-cp-surface/60">
              {row.getVisibleCells().map((cell) => (
                <td key={cell.id} className="px-4 py-3 align-top">
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
