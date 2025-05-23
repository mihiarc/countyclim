# Dask Configuration Guide for Climate Processing

This guide helps you configure and optimize Dask for climate data processing workloads.

## Quick Start

1. **Analyze your system resources:**
   ```bash
   python dask_utils.py analyze
   ```

2. **Generate optimized configuration:**
   ```bash
   python dask_utils.py optimize
   ```

3. **Test your Dask setup:**
   ```bash
   python dask_utils.py test
   ```

4. **Create workspace directory:**
   ```bash
   python dask_utils.py workspace
   ```

## Configuration Overview

### Current Configuration (config.yaml)

Your system has been configured with the following optimized settings:

- **Workers**: 4 (recommended for 8-core system)
- **Memory per worker**: 3GB (total cluster memory: 12GB)
- **Threads per worker**: 2 (optimal for I/O-bound operations)
- **Memory management**: Conservative thresholds to prevent OOM errors

### Key Dask Settings Explained

#### Memory Management
```yaml
dask_config:
  memory_target: 0.8    # Target 80% memory usage per worker
  memory_spill: 0.9     # Spill to disk at 90% memory usage
  memory_pause: 0.95    # Pause computation at 95% memory usage
  memory_terminate: 0.98 # Terminate worker at 98% memory usage
```

#### Performance Tuning
```yaml
dask_config:
  threads_per_worker: 2  # Optimal for I/O bound climate data operations
  processes: true        # Use processes for better memory isolation
  work_stealing: true    # Enable load balancing between workers
```

#### Chunking Strategy
```yaml
dask_config:
  chunk_size:
    time: 365           # Process one year at a time
    spatial: auto       # Let Dask determine optimal spatial chunking
```

## Monitoring and Optimization

### Dask Dashboard

When running climate processing, access the Dask dashboard at:
- **URL**: http://localhost:8787
- **Features**: Real-time monitoring of workers, memory usage, task progress

### Performance Monitoring

Monitor system performance during processing:
```bash
# Monitor for 10 minutes
python dask_utils.py monitor --duration 10
```

### Memory Usage Guidelines

For your 16GB system:
- **Total available**: ~16GB
- **OS and other processes**: ~4GB
- **Available for Dask**: ~12GB
- **Per worker (4 workers)**: ~3GB each

## Troubleshooting Common Issues

### 1. Out of Memory Errors

**Symptoms**: Workers being killed, "KilledWorker" errors

**Solutions**:
- Reduce `max_processes` in config.yaml
- Increase `memory_limit` per worker
- Reduce chunk sizes for large datasets

```yaml
# Conservative settings for memory-constrained systems
max_processes: 2
memory_limit: 6GB
```

### 2. Slow Performance

**Symptoms**: Tasks taking longer than expected

**Solutions**:
- Check if data is spilling to disk (dashboard shows high spill activity)
- Increase memory per worker
- Optimize chunk sizes
- Check network I/O if using distributed storage

### 3. Worker Timeouts

**Symptoms**: "CommClosedError" or timeout errors

**Solutions**:
- Increase timeout values:
```yaml
dask_config:
  connect_timeout: 120s
  tcp_timeout: 120s
```

### 4. Dashboard Not Accessible

**Solutions**:
- Check if port 8787 is available
- Change dashboard port:
```yaml
dask_config:
  dashboard_port: 8788  # Use different port
```

## Advanced Configuration

### For Large Datasets (>100GB)

```yaml
max_processes: 2
memory_limit: 7GB
dask_config:
  chunk_size:
    time: 180  # Process 6 months at a time
  memory_target: 0.7
  memory_spill: 0.8
```

### For Fast SSDs

```yaml
dask_config:
  chunk_size:
    time: 730  # Process 2 years at a time
  memory_spill: 0.95  # Less aggressive spilling
```

### For Network Storage

```yaml
dask_config:
  connect_timeout: 300s
  tcp_timeout: 300s
  chunk_size:
    time: 365  # Keep moderate chunk sizes
```

## Environment Variables

Override configuration via environment variables:

```bash
# Set number of workers
export CLIMATE_MAX_PROCESSES=2

# Set memory limit
export CLIMATE_MEMORY_LIMIT=6GB

# Disable parallel processing
export CLIMATE_PARALLEL_PROCESSING=false
```

## Performance Benchmarking

### Baseline Test

Run a simple performance test:
```bash
python dask_utils.py test
```

### Full Processing Test

Time a small region processing:
```bash
time python process_climate_data.py
```

### Expected Performance

For your system processing precipitation data:
- **CONUS region**: ~30-60 minutes
- **Alaska region**: ~15-30 minutes
- **Memory usage**: Should stay under 80%
- **CPU usage**: Should utilize most cores

## Best Practices

1. **Start Conservative**: Begin with fewer workers and more memory per worker
2. **Monitor First Run**: Watch the dashboard during your first processing run
3. **Adjust Gradually**: Make small adjustments based on observed performance
4. **Test Changes**: Use `dask_utils.py test` after configuration changes
5. **Keep Logs**: Enable logging to troubleshoot issues

## Configuration Templates

### Memory-Optimized (for large datasets)
```yaml
max_processes: 2
memory_limit: 7GB
dask_config:
  memory_target: 0.7
  memory_spill: 0.8
  threads_per_worker: 3
```

### Speed-Optimized (for smaller datasets)
```yaml
max_processes: 6
memory_limit: 2GB
dask_config:
  memory_target: 0.85
  memory_spill: 0.95
  threads_per_worker: 1
```

### Balanced (recommended starting point)
```yaml
max_processes: 4
memory_limit: 3GB
dask_config:
  memory_target: 0.8
  memory_spill: 0.9
  threads_per_worker: 2
```

## Getting Help

If you encounter issues:

1. Check the Dask dashboard for worker status
2. Run system analysis: `python dask_utils.py analyze`
3. Monitor performance: `python dask_utils.py monitor`
4. Review Dask logs in `./dask-worker-space/logs/`
5. Consult the [Dask documentation](https://docs.dask.org/) for advanced topics 