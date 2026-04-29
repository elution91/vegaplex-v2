import { useQuery } from '@tanstack/react-query'
import { getVix } from '../api/vixApi'

export function useVix() {
  return useQuery({
    queryKey: ['vix'],
    queryFn: getVix,
    staleTime: 5 * 60_000,
    refetchInterval: 5 * 60_000,
    retry: 1,
  })
}
