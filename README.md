# Climate Data Processor v2.0 - Modular Edition

A professional, modular climate data processing pipeline for calculating 30-year climate normals from daily NetCDF files with optimized Dask configuration and comprehensive error handling.

## 🏗️ Modular Architecture

The processor has been completely refactored into a clean, maintainable modular structure:

```
climate_processor/
├── main.py                          # Main entry point (~100 lines)
├── climate_processor/               # Core processing package
│   ├── __init__.py                 # Package initialization
│   ├── config.py                   # Configuration management
│   ├── data_processor.py           # Main processing orchestrator
│   ├── file_handler.py             # File operations
│   ├── regions.py                  # Regional definitions
│   ├── dask_utils.py              # Dask optimization
│   ├── validation.py              # Data validation
│   ├── coordinates.py             # Coordinate systems
│   ├── climate_calculations.py    # Climate computations
│   └── io_utils.py                # I/O operations
├── utils/                          # Utility modules
│   ├── logging_utils.py           # Enhanced logging
│   ├── memory_utils.py            # Memory monitoring
│   └── diagnostics.py            # System diagnostics
└── tests/                         # Test modules
    └── ...
```

## ✨ Key Benefits of Modular Design

### 🔧 **Maintainability**
- Each module has a single, clear responsibility
- Easy to understand, modify, and extend
- Clear interfaces between components
- Comprehensive documentation per module

### 🧪 **Testability**
- Individual modules can be unit tested
- Isolated functionality for better debugging
- Mock-friendly interfaces for testing

### 🔄 **Reusability**
- Components can be reused in other projects
- Easy to swap implementations
- Clean dependency management

### 📈 **Scalability**
- Add new features without touching existing code
- Plugin-style architecture for extensions
- Easy to add new regions or processing methods

## 🚀 Quick Start

### Installation
```bash
git clone <repository>
cd climate_processor
pip install -r requirements.txt
```

### Basic Usage
```bash
# Run with defaults
python main.py

# Run diagnostics first
python main.py --diagnostics

# Override configuration
python main.py --scenario ssp245 --variables "tas,pr"
```

## 📋 Module Overview

### Core Modules

| Module | Purpose | Lines | Key Functions |
|--------|---------|-------|---------------|
| `main.py` | Entry point & CLI | ~100 | `main()`, argument parsing |
| `config.py` | Configuration management | ~200 | `load_configuration()`, validation |
| `data_processor.py` | Processing orchestrator | ~300 | `ClimateDataProcessor` class |
| `file_handler.py` | File operations | ~400 | File discovery, validation |
| `regions.py` | Regional operations | ~250 | Regional extraction, CRS info |
| `dask_utils.py` | Dask optimization | ~200 | Cluster setup, resource config |
| `climate_calculations.py` | Climate computations | ~300 | Climate normals, statistics |
| `validation.py` | Data validation | ~150 | File & data quality checks |
| `coordinates.py` | Coordinate systems | ~150 | Coordinate conversions |
| `io_utils.py` | I/O operations | ~250 | Optimized NetCDF saving |

### Utility Modules

| Module | Purpose | Key Features |
|--------|---------|--------------|
| `logging_utils.py` | Enhanced logging | File & console output, log levels |
| `memory_utils.py` | Memory monitoring | Real-time tracking, recommendations |
| `diagnostics.py` | System diagnostics | Comprehensive system analysis |

## 🔧 Configuration

The processor uses a hierarchical configuration system:

1. **Default values** (built into code)
2. **Configuration file** (`climate_config.ini`)
3. **Command line arguments** (highest priority)

### Example Configuration
```ini
[paths]
external_drive_path = /path/to/climate/data
output_dir = output/climate_means

[processing]
active_scenario = historical
batch_size = 20
max_workers = 4

[variables]
variables = tas,tasmax,tasmin,pr
```

## 🧪 Testing Strategy

Each module can be tested independently:

```python
# Example unit test
from climate_processor.regions import extract_region, REGION_BOUNDS
from climate_processor.validation import validate_netcdf_structure

def test_region_extraction():
    # Test regional data extraction
    pass

def test_file_validation():
    # Test NetCDF file validation
    pass
```

## 🔌 Extending the Processor

### Adding New Regions
```python
# In climate_processor/regions.py
REGION_BOUNDS['NEW_REGION'] = {
    'name': 'New Region',
    'lon_min': 240, 'lon_max': 250,
    'lat_min': 30.0, 'lat_max': 40.0,
    'convert_longitudes': True
}
```

### Adding New Processing Methods
```python
# Create new module: climate_processor/new_calculations.py
def custom_climate_statistic(data_array):
    # Custom processing logic
    return processed_data
```

### Adding New Output Formats
```python
# In climate_processor/io_utils.py
def save_as_zarr(dataset, output_path):
    # Custom output format
    dataset.to_zarr(output_path)
```

## 🐛 Debugging

The modular structure makes debugging much easier:

1. **Identify the failing module** from log messages
2. **Run module-specific tests** to isolate issues
3. **Use module boundaries** to narrow down problems
4. **Mock dependencies** for isolated testing

### Debug Mode
```bash
python main.py --verbose  # Enable debug logging
python main.py --diagnostics  # Run comprehensive diagnostics
```

## 📊 Performance Monitoring

Each module includes performance monitoring:

- **Memory usage** tracked at module boundaries
- **Processing time** logged for each module
- **Dask performance** monitored throughout
- **System resources** continuously tracked

## 🔄 Migration from Monolithic Version

The modular version maintains full compatibility:

- **Same CLI interface**
- **Same configuration format**
- **Same output format**
- **Enhanced performance and reliability**

## 🤝 Contributing

The modular structure makes contributions easier:

1. **Identify the relevant module** for your change
2. **Make changes within module boundaries**
3. **Add tests for your module**
4. **Update module documentation**

### Adding New Features
1. Create new module if needed
2. Update `__init__.py` files
3. Add configuration options
4. Add tests and documentation

## 📈 Performance Comparison

| Metric | Monolithic | Modular | Improvement |
|--------|------------|---------|-------------|
| Lines of code | ~1000 | ~200-400 per module | Better maintainability |
| Memory efficiency | Good | Excellent | Optimized per module |
| Debugging time | High | Low | Isolated components |
| Test coverage | Difficult | Easy | Module-level testing |
| Feature additions | Risky | Safe | Isolated changes |

## 🔐 Error Handling

Each module has comprehensive error handling:

- **Module-level exception handling**
- **Graceful degradation**
- **Detailed error reporting**
- **Recovery mechanisms**

## 📚 Documentation

Each module is self-documenting:

- **Comprehensive docstrings**
- **Type hints throughout**
- **Usage examples**
- **API documentation**

This modular architecture transforms the climate processor from a monolithic script into a professional, maintainable software package that's easy to understand, test, and extend.