# Precipitation Data Conversion

## Converting Precipitation Flux (pr) to Millimeters (mm)

Climate model outputs often provide precipitation as a flux in units of `kg m⁻² s⁻¹` (kilograms per square meter per second). To convert this to millimeters (mm) of precipitation over a given time period, use the following method:

### Why This Works
- 1 kg of water over 1 m² is equivalent to a 1 mm layer of water (since 1 liter = 1 kg = 1 mm over 1 m²).
- The `s⁻¹` (per second) means the value is a rate, so you need to multiply by the number of seconds in your desired time period.

### Formula
For daily precipitation:

```
pr (mm/day) = pr (kg m⁻² s⁻¹) × 86400
```

Where 86400 is the number of seconds in a day (24 × 60 × 60).

### Example
If the model output is `0.0001 kg m⁻² s⁻¹`, then:

```
0.0001 × 86400 = 8.64 mm/day
```

This is the standard approach used in climate science (see CMIP/ERA5/ECMWF documentation). 

---

# Temperature Conversion

## Kelvin to Celsius

To convert temperature from Kelvin (K) to Celsius (°C):

```
T(°C) = T(K) - 273.15
```

**Example:**
If the model output is `295.15 K`, then:
```
295.15 - 273.15 = 22 °C
```

## Kelvin to Fahrenheit

To convert temperature from Kelvin (K) to Fahrenheit (°F):

```
T(°F) = (T(K) - 273.15) × 9/5 + 32
```

**Example:**
If the model output is `295.15 K`, then:
```
(295.15 - 273.15) × 9/5 + 32 = 71.6 °F
``` 