import ReactECharts from 'echarts-for-react'

interface Props {
  option: Record<string, unknown>
  height?: string | number
  className?: string
  loading?: boolean
}

const LOADING_OPTS = {
  text: '',
  color: '#58a6ff',
  textColor: '#8b949e',
  maskColor: 'rgba(13,17,23,0.6)',
  zlevel: 0,
}

export default function EChart({ option, height = 260, className, loading = false }: Props) {
  return (
    <ReactECharts
      option={option}
      style={{ height, width: '100%' }}
      className={className}
      showLoading={loading}
      loadingOption={LOADING_OPTS}
      notMerge
      lazyUpdate={false}
      theme="dark"
    />
  )
}
