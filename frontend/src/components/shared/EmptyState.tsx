interface Props { message?: string }

export default function EmptyState({ message = 'No data' }: Props) {
  return (
    <div className="flex items-center justify-center h-32 text-text-muted text-xs">
      {message}
    </div>
  )
}
