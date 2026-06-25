"""
Generate synthetic incident reports for the RAG knowledge base.

Creates JSON files in data/incidents/ covering a variety of failure modes
relevant to telemetry monitoring (temperature, humidity, pressure, cpu_usage).
"""

import json
import os

INCIDENTS = [
    {
        "filename": "006_temperature_ambient_heatwave.json",
        "title": "Ambient temperature rise during regional heatwave affecting outdoor sensors",
        "description": "Outdoor-mounted temperature sensors across the facility perimeter reported readings 15-20°C above normal seasonal baselines over a 5-day period. Indoor sensors remained stable. Weather service data confirmed an extreme heat event in the region with ambient temperatures exceeding 45°C.",
        "affected_device_types": ["temperature_sensor", "outdoor_monitor"],
        "root_cause": "Legitimate environmental condition — regional heatwave caused actual ambient temperature increase. Sensors were reporting accurately. The anomaly detection system correctly identified the deviation from baseline but the root cause was external weather rather than equipment failure.",
        "resolution_steps": [
            "Cross-reference with local weather station data to confirm environmental cause",
            "Verify sensor accuracy by comparing multiple co-located sensors",
            "Temporarily adjust alert thresholds for outdoor sensors during extreme weather events",
            "Confirm no equipment damage occurred due to sustained high temperatures",
            "Review thermal protection ratings of outdoor equipment",
            "Consider implementing weather-aware dynamic thresholds for outdoor sensors"
        ],
        "severity": "low",
        "failure_category": "environmental",
        "tags": ["temperature", "weather", "external_cause", "false_positive", "seasonal"]
    },
    {
        "filename": "007_cpu_cryptominer_compromise.json",
        "title": "Unexpected CPU usage spike from unauthorized cryptocurrency mining process",
        "description": "A single edge gateway device reported CPU usage jumping from a baseline of 20% to sustained 95-100% over a 4-hour period. The spike was isolated to one device while identical devices in the same deployment remained at normal levels. Network traffic analysis showed unusual outbound connections to unknown IP addresses on non-standard ports.",
        "affected_device_types": ["edge_gateway", "iot_controller"],
        "root_cause": "The device was compromised through an unpatched vulnerability in the web management interface (CVE-2025-XXXX). An attacker deployed a cryptocurrency mining payload that consumed all available CPU resources. The isolated nature of the spike (single device) and the network traffic pattern are characteristic indicators of cryptojacking.",
        "resolution_steps": [
            "Immediately isolate the affected device from the network",
            "Capture forensic image of device storage before remediation",
            "Identify the attack vector by reviewing access logs and open ports",
            "Reimage the device with verified clean firmware",
            "Apply security patches for all known vulnerabilities before reconnecting",
            "Audit all devices of the same type for similar indicators of compromise",
            "Implement network segmentation to limit lateral movement",
            "Add outbound connection monitoring rules for non-standard ports"
        ],
        "severity": "critical",
        "failure_category": "security_incident",
        "tags": ["cpu_usage", "security", "cryptominer", "single_device", "network_anomaly"]
    },
    {
        "filename": "008_humidity_sensor_end_of_life.json",
        "title": "Gradual humidity sensor degradation indicating end-of-life failure mode",
        "description": "A humidity sensor deployed for 4 years began reporting increasingly erratic readings over a 2-week period. Values oscillated between 20% and 80% RH without corresponding environmental changes. Adjacent sensors reported stable readings throughout. The sensor's self-diagnostic reported no errors.",
        "affected_device_types": ["humidity_sensor", "environmental_monitor"],
        "root_cause": "Capacitive humidity sensors have a finite operational lifespan (typically 5-7 years). The sensing element's polymer layer degrades over time due to exposure to ambient contaminants, causing increased hysteresis and eventually random output. This device was approaching its rated end-of-life at 4 years in a high-particulate environment that accelerates degradation.",
        "resolution_steps": [
            "Compare affected sensor readings against adjacent sensors to confirm isolated failure",
            "Check device deployment date against manufacturer's rated lifespan",
            "Replace the sensor with a new unit of the same model",
            "Calibrate the replacement sensor against a reference standard",
            "Update device registry with new sensor serial number and installation date",
            "Implement proactive replacement schedule based on manufacturer's rated lifespan minus 20% safety margin"
        ],
        "severity": "medium",
        "failure_category": "hardware_degradation",
        "tags": ["humidity", "end_of_life", "degradation", "single_device", "erratic_readings"]
    },
    {
        "filename": "009_pressure_rapid_drop_door_event.json",
        "title": "Sudden pressure drop in cleanroom triggered by airlock door malfunction",
        "description": "Pressure sensors in Cleanroom B reported a sudden 30% drop in differential pressure, falling below the required positive-pressure threshold. The drop occurred over approximately 10 seconds and triggered immediate contamination risk alerts. The event coincided with a logged entry into the cleanroom airlock.",
        "affected_device_types": ["pressure_sensor", "cleanroom_monitor"],
        "root_cause": "The cleanroom airlock interlock mechanism failed to prevent both doors from being open simultaneously. When the inner door opened before the outer door fully sealed, the pressure differential equalized with the corridor. The interlock failure was caused by a misaligned door sensor magnet that reported the outer door as closed when it was ajar by 2cm.",
        "resolution_steps": [
            "Immediately initiate cleanroom contamination assessment protocol",
            "Inspect airlock door sensors and interlock mechanism",
            "Realign or replace the door position sensor magnet",
            "Verify interlock prevents simultaneous door opening after repair",
            "Review pressure recovery time to confirm HVAC system is adequately sized",
            "Add secondary door position verification (optical sensor backup)",
            "Test interlock mechanism under various door positions to confirm reliable detection"
        ],
        "severity": "critical",
        "failure_category": "mechanical_failure",
        "tags": ["pressure", "cleanroom", "door_malfunction", "rapid_change", "contamination_risk"]
    },
    {
        "filename": "010_temperature_network_delay_batch.json",
        "title": "Apparent temperature spike caused by network buffering and batch delivery",
        "description": "A temperature sensor reported 15 identical readings simultaneously, all showing values 10°C above the previous stable reading. The batch arrived with identical timestamps despite the device normally reporting every 30 seconds. Other sensors in the same zone showed no change.",
        "affected_device_types": ["temperature_sensor", "wireless_sensor"],
        "root_cause": "The wireless sensor experienced network connectivity loss for approximately 7 minutes. The device firmware buffers readings locally when connectivity is lost. Upon reconnection, all buffered readings were transmitted simultaneously with their original timestamps. The apparently anomalous readings were actually legitimate measurements taken during a brief localized heat event (nearby equipment startup) that resolved before connectivity returned.",
        "resolution_steps": [
            "Check device connectivity logs for gaps corresponding to the batch timestamp window",
            "Verify that the batch count multiplied by the reporting interval matches the connectivity gap duration",
            "Review actual reading values in context of their original timestamps rather than delivery time",
            "If readings represent a real event, investigate the localized heat source active during that period",
            "Configure anomaly detection to account for batch-delivered readings (detect and flag network recovery patterns)",
            "Consider implementing delivery-time vs measurement-time distinction in the ingestion pipeline"
        ],
        "severity": "low",
        "failure_category": "network_connectivity",
        "tags": ["temperature", "network", "buffering", "batch_delivery", "false_positive", "wireless"]
    },
    {
        "filename": "011_cpu_firmware_update_spike.json",
        "title": "CPU usage spike during scheduled over-the-air firmware update",
        "description": "Multiple IoT devices reported CPU usage spikes to 80-90% lasting 3-5 minutes each. The spikes occurred in a rolling pattern across devices over a 30-minute window. All affected devices were in the same firmware update group and the timing aligned with the scheduled OTA update window.",
        "affected_device_types": ["edge_gateway", "iot_controller", "temperature_sensor"],
        "root_cause": "The over-the-air firmware update process requires significant CPU resources for downloading, verifying (cryptographic signature check), and flashing the new firmware image. The staggered pattern is by design — the update system rolls updates across devices in batches to avoid simultaneous resource consumption. CPU usage returns to normal baseline after the update completes.",
        "resolution_steps": [
            "Correlate CPU spikes with OTA update schedule to confirm expected behavior",
            "Verify all devices successfully completed the update (check firmware version post-update)",
            "If any devices show sustained high CPU after update window, investigate failed update requiring retry",
            "Add suppression rule for CPU alerts during scheduled OTA update windows",
            "Document expected CPU impact of firmware updates for operator awareness",
            "Consider scheduling OTA updates during low-activity periods to minimize alert noise"
        ],
        "severity": "low",
        "failure_category": "scheduled_maintenance",
        "tags": ["cpu_usage", "firmware_update", "expected_behavior", "multi_device", "rolling_pattern"]
    },
    {
        "filename": "012_temperature_direct_sunlight.json",
        "title": "Temperature sensor reading elevated due to direct solar radiation exposure",
        "description": "An outdoor temperature sensor consistently reports readings 8-12°C above the regional weather station baseline during afternoon hours (1pm-5pm). Morning and evening readings align with expected values. The pattern repeats daily with consistent timing. The sensor was recently relocated as part of a facility expansion.",
        "affected_device_types": ["temperature_sensor", "outdoor_monitor"],
        "root_cause": "The sensor was relocated to a position that receives direct afternoon sunlight. The sensor housing absorbs solar radiation, heating the sensing element above actual ambient temperature. This is a well-known measurement error called 'solar loading' that affects improperly shielded temperature sensors. The time pattern corresponds to the sun's position relative to the sensor mounting location.",
        "resolution_steps": [
            "Verify the daily pattern correlates with solar position (worst during clear-sky afternoons)",
            "Install a solar radiation shield (Stevenson screen or aspirated shield) over the sensor",
            "If immediate shielding is unavailable, relocate sensor to a north-facing shaded position",
            "Apply a correction factor to readings during affected hours as a temporary measure",
            "Update installation guidelines to require solar shielding assessment for all outdoor sensor placements",
            "Validate correction by comparing shielded readings against a reference sensor"
        ],
        "severity": "low",
        "failure_category": "installation_error",
        "tags": ["temperature", "solar_loading", "installation", "time_pattern", "outdoor"]
    },
    {
        "filename": "013_humidity_water_ingress.json",
        "title": "Humidity sensor reading 100% sustained due to water ingress into housing",
        "description": "A single humidity sensor in the basement utility room reports a constant 100% RH reading. The reading does not fluctuate and has remained fixed at maximum for 48 hours. Adjacent sensors report normal readings of 55-60% RH. The sensor housing shows no visible external damage.",
        "affected_device_types": ["humidity_sensor", "environmental_monitor"],
        "root_cause": "Water ingress through a degraded cable gland seal allowed moisture to accumulate inside the sensor housing. The internal condensation saturated the sensing element, causing it to report maximum humidity regardless of ambient conditions. The cable gland O-ring had degraded due to age and exposure to cleaning chemicals used in the utility room.",
        "resolution_steps": [
            "Remove sensor from mounting and inspect housing for internal moisture",
            "Dry the sensor completely in a desiccant chamber for 24-48 hours",
            "Replace the cable gland seal with a chemical-resistant variant (e.g., EPDM or Viton)",
            "Test sensor after drying; replace if readings do not return to accurate values",
            "Apply conformal coating to PCB if water contact is suspected",
            "Audit all sensors in chemical-exposure areas for seal integrity",
            "Upgrade to IP67-rated housings in areas where cleaning chemicals are used"
        ],
        "severity": "medium",
        "failure_category": "hardware_degradation",
        "tags": ["humidity", "water_ingress", "seal_failure", "stuck_reading", "single_device"]
    },
    {
        "filename": "014_cpu_dos_attack_pattern.json",
        "title": "CPU exhaustion from denial-of-service request flood targeting management API",
        "description": "Three edge gateway devices simultaneously reported CPU usage exceeding 95% with corresponding increases in network interface utilization. The CPU spike was sustained and did not follow any scheduled activity pattern. Network logs showed an unusually high rate of incoming connections on the device management port from a single source IP.",
        "affected_device_types": ["edge_gateway", "iot_controller"],
        "root_cause": "A misconfigured network scanner in the IT department was running an aggressive vulnerability scan against the IoT device subnet. The scan sent thousands of connection attempts per second to each device's management port, overwhelming the limited CPU resources of the embedded devices. The scanner was configured for enterprise server scanning rates inappropriate for resource-constrained IoT devices.",
        "resolution_steps": [
            "Identify the source IP generating excessive traffic from network logs",
            "Contact the source owner to halt the scan immediately",
            "Implement rate limiting on device management interfaces (max 10 connections/second)",
            "Add the IoT subnet to the vulnerability scanner's exclusion list or reduce scan intensity",
            "Configure network-level rate limiting on the switch/firewall for the IoT VLAN",
            "Implement connection timeout limits on device management interfaces",
            "Schedule any required vulnerability scans during maintenance windows with reduced intensity settings"
        ],
        "severity": "high",
        "failure_category": "network_overload",
        "tags": ["cpu_usage", "network", "dos", "vulnerability_scan", "multi_device", "simultaneous"]
    },
    {
        "filename": "015_pressure_altitude_compensation_error.json",
        "title": "Pressure readings offset after device relocation to different elevation",
        "description": "A pressure sensor was relocated from a ground-floor installation to a rooftop equipment room (15 meters higher elevation). Post-relocation, readings show a consistent offset of approximately -180 Pa from expected values. The offset is constant regardless of weather conditions.",
        "affected_device_types": ["pressure_sensor", "environmental_monitor"],
        "root_cause": "The device was configured with altitude compensation for its original ground-floor installation. After relocation to the rooftop (15m elevation change), the altitude compensation was not updated. Atmospheric pressure decreases approximately 12 Pa per meter of elevation gain, resulting in the ~180 Pa offset. The sensor is reading correctly for its new altitude but the compensation setting creates an apparent error.",
        "resolution_steps": [
            "Verify the elevation change between old and new installation locations",
            "Calculate expected pressure offset (approximately 12 Pa per meter of elevation)",
            "Update the device's altitude compensation setting to reflect new installation height",
            "Verify corrected readings against a calibrated reference barometer at the same location",
            "Update the device registry with new location and elevation metadata",
            "Add altitude compensation verification to the sensor relocation checklist"
        ],
        "severity": "low",
        "failure_category": "configuration_error",
        "tags": ["pressure", "calibration", "relocation", "altitude", "constant_offset", "configuration"]
    },
    {
        "filename": "016_temperature_thermal_coupling_adjacent_equipment.json",
        "title": "Temperature sensor affected by thermal radiation from newly installed adjacent equipment",
        "description": "A temperature sensor monitoring ambient room conditions began reporting readings 5-8°C above baseline after new server hardware was installed in an adjacent rack. The elevation correlates with server workload patterns — higher during business hours, returning closer to baseline at night. The sensor is wall-mounted 2 meters from the new rack.",
        "affected_device_types": ["temperature_sensor", "room_monitor"],
        "root_cause": "The newly installed server rack generates significant thermal radiation. The temperature sensor's proximity (2 meters) and direct line-of-sight to the rack exhaust means it measures a combination of ambient air temperature and radiated heat from the servers. The workload correlation confirms the source — server heat output follows processing demand patterns.",
        "resolution_steps": [
            "Verify correlation between elevated readings and server rack utilization metrics",
            "Relocate sensor to a position shielded from direct thermal radiation (behind a partition or farther from heat sources)",
            "If relocation is impractical, install a radiation shield between the sensor and heat source",
            "Alternatively, add a second sensor in a thermally neutral position and use the average",
            "Update baseline expectations if the sensor must remain in its current position",
            "Document minimum distance requirements between temperature sensors and heat-generating equipment"
        ],
        "severity": "low",
        "failure_category": "installation_error",
        "tags": ["temperature", "thermal_coupling", "equipment_change", "workload_correlation", "installation"]
    },
    {
        "filename": "017_multi_sensor_power_supply_noise.json",
        "title": "Multiple sensor types showing correlated noise pattern from shared power supply ripple",
        "description": "Temperature, humidity, and pressure sensors on the same monitoring panel simultaneously began showing ±3-5% oscillations at a regular 2-second interval. The oscillation pattern is identical across all three sensor types and appeared after a UPS battery replacement. Sensors on other panels are unaffected.",
        "affected_device_types": ["temperature_sensor", "humidity_sensor", "pressure_sensor"],
        "root_cause": "The replacement UPS battery has higher internal resistance than the original, causing increased voltage ripple on the sensor power rail during the inverter switching cycle (0.5 Hz switching frequency = 2-second oscillation period). The analog sensor circuits lack sufficient power supply rejection at this frequency, coupling the ripple into measurement outputs. The correlated pattern across sensor types confirms a shared power supply issue rather than independent sensor faults.",
        "resolution_steps": [
            "Measure power supply rail voltage with an oscilloscope to confirm ripple presence and frequency",
            "Verify UPS battery specifications match the original unit's requirements",
            "If battery specs are correct, add a linear voltage regulator between UPS output and sensor power rail",
            "Alternatively, add additional capacitance (electrolytic + ceramic) at the sensor power input",
            "Test replacement with the correct battery specification if available",
            "Add power quality monitoring to critical sensor power circuits"
        ],
        "severity": "medium",
        "failure_category": "power_quality",
        "tags": ["multi_sensor", "power_supply", "oscillation", "correlated", "ups", "noise"]
    },
    {
        "filename": "018_cpu_garbage_collection_spikes.json",
        "title": "Periodic CPU spikes from Java-based gateway garbage collection pauses",
        "description": "Edge gateway devices running the Java-based telemetry agent show periodic CPU spikes to 70-85% lasting 2-5 seconds every 15-30 minutes. Between spikes, CPU usage remains normal at 15-20%. The spikes do not correlate with telemetry load or external events. All devices running the Java agent exhibit this pattern.",
        "affected_device_types": ["edge_gateway", "iot_controller"],
        "root_cause": "The Java Virtual Machine's garbage collector performs periodic full GC cycles to reclaim unused memory. The default GC configuration (Parallel GC with 256MB max heap) causes stop-the-world pauses that consume significant CPU on the resource-constrained embedded hardware. The 15-30 minute interval corresponds to the heap filling rate at normal telemetry throughput.",
        "resolution_steps": [
            "Confirm GC as root cause by enabling verbose GC logging and correlating pause timestamps with CPU spikes",
            "Tune JVM parameters: switch to G1GC (-XX:+UseG1GC) with smaller max pause target (-XX:MaxGCPauseMillis=200)",
            "Reduce heap size if possible to decrease individual GC duration",
            "Increase GC frequency with smaller generations to prevent large full-GC pauses",
            "Consider migrating the telemetry agent to a non-GC language for embedded deployments",
            "If pattern is acceptable, add suppression rule to ignore CPU spikes under 10 seconds duration"
        ],
        "severity": "low",
        "failure_category": "software_configuration",
        "tags": ["cpu_usage", "garbage_collection", "java", "periodic", "expected_behavior", "configuration"]
    },
    {
        "filename": "019_temperature_sensor_wire_fault.json",
        "title": "Temperature sensor reporting fixed value due to thermocouple wire break",
        "description": "A thermocouple-based temperature sensor suddenly began reporting a fixed value of -40°C regardless of actual ambient conditions. The reading appeared instantaneously and has not varied since. The device's self-diagnostic reports no communication errors.",
        "affected_device_types": ["temperature_sensor", "industrial_monitor"],
        "root_cause": "The thermocouple wire developed an open circuit (break) at the junction box terminal. When a thermocouple circuit is open, the measurement circuit reads a voltage corresponding to the cold-junction temperature of the measurement IC, which is typically calibrated to report -40°C as a fault indication. The break was caused by corrosion at the terminal due to moisture exposure in the junction box.",
        "resolution_steps": [
            "Recognize -40°C fixed reading as a thermocouple open-circuit fault signature",
            "Inspect thermocouple wiring from the sensor head to the measurement terminal",
            "Check junction box for moisture, corrosion, or mechanical damage to terminals",
            "Re-terminate or replace the thermocouple wire as needed",
            "Apply dielectric grease to terminal connections to prevent future corrosion",
            "Seal junction box against moisture ingress",
            "Configure alerting to recognize fixed min/max value readings as sensor fault patterns"
        ],
        "severity": "high",
        "failure_category": "wiring_fault",
        "tags": ["temperature", "thermocouple", "open_circuit", "fixed_value", "wiring", "corrosion"]
    },
    {
        "filename": "020_humidity_rapid_cycling.json",
        "title": "Humidity sensor rapid cycling between two values indicating ADC failure",
        "description": "A humidity sensor alternates between exactly 32.0% and 64.0% RH on every consecutive reading (every 30 seconds). The values do not vary and the pattern has persisted for 6 hours. Temperature and other co-located sensors report stable, normal values.",
        "affected_device_types": ["humidity_sensor", "environmental_monitor"],
        "root_cause": "The sensor's analog-to-digital converter (ADC) has a stuck bit (bit 7) that alternates between 0 and 1 on consecutive conversions. This creates two fixed output values that are exactly one binary power apart (32% = 0100000, 64% = 1000000 in the 7-bit humidity range). This is a known failure mode of successive-approximation ADCs when a comparator input develops a marginal connection.",
        "resolution_steps": [
            "Confirm the binary pattern by converting reported values to raw ADC counts",
            "Power cycle the sensor to determine if the stuck bit clears (sometimes a temporary latch-up)",
            "If power cycling resolves, monitor for recurrence (indicates marginal solder joint)",
            "If persistent after power cycle, replace the sensor unit",
            "Log the failure mode for future pattern recognition (two alternating fixed values = ADC bit fault)",
            "Consider implementing a reading validation rule that flags constant alternating-value patterns"
        ],
        "severity": "medium",
        "failure_category": "hardware_failure",
        "tags": ["humidity", "adc_failure", "stuck_bit", "alternating_values", "hardware"]
    },
    {
        "filename": "021_temperature_cascade_cooling_failure.json",
        "title": "Cascading temperature rise across data center floor due to chiller failure",
        "description": "Temperature sensors across an entire data center floor (40+ sensors) reported progressive temperature increases over a 20-minute period. The pattern started from sensors nearest the CRAC units and propagated outward. Temperatures rose from 22°C baseline to 35-42°C depending on proximity to high-density racks. Humidity sensors simultaneously reported dropping values as relative humidity decreased with rising temperature.",
        "affected_device_types": ["temperature_sensor", "humidity_sensor", "rack_monitor"],
        "root_cause": "The primary chiller plant experienced a refrigerant compressor failure, eliminating cold water supply to all CRAC units on the affected floor. CRAC units continued running fans (circulating air) but without cooling capacity. The propagation pattern from CRAC units outward reflects the loss of cold air supply at those points. The secondary chiller took 18 minutes to start and reach capacity due to a control system sequencing delay.",
        "resolution_steps": [
            "Initiate emergency cooling protocol: open exterior doors/windows if weather permits, deploy portable cooling units",
            "Reduce heat load by identifying and shutting down non-critical equipment",
            "Diagnose chiller failure: check compressor, refrigerant levels, electrical supply",
            "Verify secondary chiller auto-start sequence is configured with acceptable delay",
            "Reduce secondary chiller start delay from 18 minutes to under 5 minutes",
            "Monitor all equipment temperatures during recovery to identify any thermal damage",
            "Conduct post-incident review of cooling redundancy adequacy"
        ],
        "severity": "critical",
        "failure_category": "cooling_system_failure",
        "tags": ["temperature", "humidity", "cooling", "cascade", "multi_device", "data_center", "critical_infrastructure"]
    },
    {
        "filename": "022_pressure_transient_compressor_startup.json",
        "title": "Pressure transient during HVAC compressor startup misidentified as anomaly",
        "description": "Pressure sensors in the HVAC-monitored zone report brief spikes of 5-10% above baseline lasting 3-8 seconds, occurring at irregular intervals throughout the day. The transients appear to have no pattern but always resolve immediately.",
        "affected_device_types": ["pressure_sensor", "hvac_monitor"],
        "root_cause": "Each transient corresponds to an HVAC compressor start cycle. When the compressor engages, it creates a momentary pressure pulse in the ductwork before the system reaches steady-state airflow. The irregular timing corresponds to the thermostat-driven on/off cycling of the HVAC system. These are normal operational transients that are too brief to affect room conditions but are captured by the high-frequency pressure sensors.",
        "resolution_steps": [
            "Correlate pressure transients with HVAC compressor start events from the building management system",
            "Implement a minimum duration filter: only alert on pressure deviations sustained for more than 30 seconds",
            "Alternatively, apply a moving average or low-pass filter to pressure readings to smooth transients",
            "Document expected transient characteristics for operator training",
            "If transients exceed 15% magnitude, investigate ductwork sizing or damper configuration",
            "Consider reducing sensor sampling rate if sub-second transients are not operationally relevant"
        ],
        "severity": "low",
        "failure_category": "expected_behavior",
        "tags": ["pressure", "transient", "hvac", "compressor", "false_positive", "brief_duration"]
    },
    {
        "filename": "023_cpu_telemetry_backlog_processing.json",
        "title": "CPU spike during telemetry backlog processing after network restoration",
        "description": "An edge gateway device reported CPU usage of 90-95% sustained for 12 minutes following a 2-hour network outage. The device buffered telemetry locally during the outage and attempted to transmit all buffered data simultaneously upon reconnection. Normal baseline CPU is 15-20%.",
        "affected_device_types": ["edge_gateway", "iot_controller"],
        "root_cause": "The device firmware's store-and-forward mechanism accumulated approximately 7,200 telemetry events during the 2-hour outage (at 1 event/second). Upon network restoration, the firmware's backlog processor attempted to serialize, compress, and transmit all buffered events as fast as possible. The combination of JSON serialization and TLS encryption for bulk transmission saturated the embedded CPU.",
        "resolution_steps": [
            "Verify the CPU spike duration corresponds to estimated backlog processing time",
            "Implement rate-limiting on backlog transmission: maximum 100 events per second during catch-up",
            "Add exponential backoff between backlog batches to prevent sustained CPU saturation",
            "Configure backlog processing to run at reduced priority (nice level) to preserve headroom for real-time operations",
            "Add telemetry age metadata so downstream systems can identify and appropriately handle delayed readings",
            "Implement a maximum backlog size with oldest-first eviction to prevent unbounded growth during extended outages"
        ],
        "severity": "medium",
        "failure_category": "network_recovery",
        "tags": ["cpu_usage", "network", "backlog", "store_and_forward", "burst", "recovery"]
    },
    {
        "filename": "024_temperature_emi_interference.json",
        "title": "Temperature sensor noise caused by electromagnetic interference from VFD installation",
        "description": "A temperature sensor installed near industrial equipment began showing erratic high-frequency noise (±5°C fluctuations at sub-second intervals) after a new Variable Frequency Drive (VFD) was installed 3 meters away. Readings were stable before the VFD installation. The noise pattern follows the VFD operating schedule.",
        "affected_device_types": ["temperature_sensor", "industrial_monitor"],
        "root_cause": "The VFD's pulse-width modulation switching (typically 4-16 kHz) generates electromagnetic interference that couples into the temperature sensor's analog signal wiring. The unshielded sensor cable acts as an antenna, picking up the EMI and introducing noise into the measurement. The correlation with VFD operating schedule confirms the interference source.",
        "resolution_steps": [
            "Confirm EMI source by correlating noise pattern with VFD start/stop schedule",
            "Replace unshielded sensor cable with shielded twisted-pair cable, properly grounded at one end",
            "Route sensor cable at least 30cm away from VFD power cables, crossing at 90° angles only",
            "Install a ferrite choke on the sensor cable near the measurement terminal",
            "If wiring changes are impractical, add a hardware low-pass filter at the sensor input",
            "Apply digital filtering (moving average of last 5 readings) in firmware as a software mitigation"
        ],
        "severity": "medium",
        "failure_category": "electromagnetic_interference",
        "tags": ["temperature", "emi", "vfd", "noise", "industrial", "wiring"]
    },
    {
        "filename": "025_multi_sensor_time_sync_drift.json",
        "title": "Apparent multi-device anomaly caused by NTP time synchronization failure",
        "description": "Multiple sensors appeared to simultaneously report anomalous values when viewed on the monitoring dashboard. However, investigation revealed the readings themselves were normal — the apparent anomaly was caused by events from different time periods being displayed together due to incorrect timestamps. Devices had drifted up to 5 minutes from actual time.",
        "affected_device_types": ["temperature_sensor", "humidity_sensor", "pressure_sensor", "edge_gateway"],
        "root_cause": "The NTP server used by the IoT device fleet became unreachable due to a firewall rule change. Without time synchronization, device clocks drifted at their individual crystal oscillator rates (typically ±20 ppm, or ~1.7 seconds per day). After several days, the cumulative drift caused time-series data to be misaligned when displayed on dashboards that assume synchronized timestamps.",
        "resolution_steps": [
            "Verify NTP server accessibility from the device network segment",
            "Restore firewall rule allowing NTP traffic (UDP port 123) to the time server",
            "Force immediate time resynchronization on all affected devices",
            "Implement NTP reachability monitoring with alerting on loss of sync",
            "Configure devices to report their sync status and estimated clock accuracy",
            "Add a secondary NTP source for redundancy",
            "Consider marking telemetry data with a clock-quality indicator for downstream processing"
        ],
        "severity": "medium",
        "failure_category": "time_synchronization",
        "tags": ["multi_sensor", "ntp", "time_drift", "false_positive", "infrastructure"]
    },
    {
        "filename": "026_temperature_self_heating.json",
        "title": "Temperature sensor self-heating error from increased measurement frequency",
        "description": "After a configuration change increased the temperature measurement frequency from once per 30 seconds to once per second, the sensor began reporting readings consistently 2-3°C above the reference thermometer. The offset appeared immediately after the configuration change and is constant regardless of ambient temperature.",
        "affected_device_types": ["temperature_sensor", "environmental_monitor"],
        "root_cause": "The sensor's excitation current (used to measure the resistance of the RTD element) generates heat within the sensing element itself. At the original 30-second measurement interval, the element had time to dissipate this heat between readings. At 1-second intervals, the cumulative self-heating raises the element temperature above ambient, creating a systematic positive offset. This is a documented characteristic of RTD sensors specified in the datasheet.",
        "resolution_steps": [
            "Review sensor datasheet for self-heating specification at the configured excitation current",
            "Reduce measurement frequency back to a rate that allows thermal dissipation (typically 10+ seconds between readings)",
            "If high-frequency measurement is required, reduce excitation current (may reduce measurement resolution)",
            "Apply a self-heating correction factor based on the manufacturer's specified thermal resistance",
            "Consider using a sensor with lower self-heating specification (e.g., thin-film RTD vs wire-wound)",
            "Document maximum recommended measurement frequency for each sensor type in deployment guidelines"
        ],
        "severity": "low",
        "failure_category": "configuration_error",
        "tags": ["temperature", "self_heating", "configuration", "measurement_frequency", "offset"]
    },
    {
        "filename": "027_cpu_log_rotation_spike.json",
        "title": "Daily CPU usage spike at midnight caused by log rotation and compression",
        "description": "Edge gateway devices consistently show a CPU spike to 60-70% at exactly 00:00 UTC every day, lasting 30-90 seconds. The spike is predictable and repeatable. It does not correlate with any telemetry load changes or external events.",
        "affected_device_types": ["edge_gateway", "iot_controller"],
        "root_cause": "The device's logrotate cron job triggers at midnight UTC, compressing the previous day's application logs using gzip. The compression operation is CPU-intensive on the embedded processor, particularly when log verbosity is set to DEBUG and the daily log file exceeds 50MB. The duration varies based on log file size, which depends on telemetry throughput that day.",
        "resolution_steps": [
            "Confirm by checking cron schedule and correlating with spike timing",
            "Reduce log verbosity from DEBUG to INFO in production deployments to decrease log file size",
            "Change compression algorithm from gzip to lz4 (faster with acceptable compression ratio)",
            "Schedule log rotation during a known low-activity period for the specific deployment",
            "Implement log size limits to cap the maximum file size requiring compression",
            "Add this known pattern to runbook documentation so operators can recognize it without investigation"
        ],
        "severity": "low",
        "failure_category": "scheduled_maintenance",
        "tags": ["cpu_usage", "log_rotation", "cron", "periodic", "predictable", "expected_behavior"]
    },
    {
        "filename": "028_humidity_hvac_hunting.json",
        "title": "Humidity oscillation caused by HVAC control loop hunting",
        "description": "Humidity sensors in a climate-controlled room show regular oscillations of ±8% RH with a period of approximately 10 minutes. The oscillation is sinusoidal and continuous, never settling to a stable value. Temperature sensors show a corresponding but smaller oscillation of ±0.5°C.",
        "affected_device_types": ["humidity_sensor", "temperature_sensor", "hvac_monitor"],
        "root_cause": "The HVAC proportional-integral-derivative (PID) controller has been tuned with excessive proportional gain for the room's thermal characteristics. This causes the control loop to overshoot the setpoint, then overcorrect in the opposite direction — a condition known as 'hunting.' The 10-minute period corresponds to the thermal time constant of the room. The humidity oscillation is larger because the dehumidification system has binary (on/off) control rather than proportional modulation.",
        "resolution_steps": [
            "Confirm hunting by plotting humidity and HVAC valve position over time (expect inverse correlation with phase lag)",
            "Reduce PID proportional gain (Kp) by 30-50% as initial adjustment",
            "Increase integral time constant (Ti) to reduce overshoot tendency",
            "If available, enable proportional dehumidification control instead of binary mode",
            "After PID adjustment, monitor for 24 hours to confirm oscillation amplitude decreases",
            "If auto-tuning is available on the HVAC controller, initiate it during stable ambient conditions"
        ],
        "severity": "medium",
        "failure_category": "control_system",
        "tags": ["humidity", "temperature", "hvac", "oscillation", "pid_tuning", "hunting", "control_loop"]
    },
    {
        "filename": "029_pressure_leak_gradual.json",
        "title": "Gradual pressure decrease indicating developing pneumatic system leak",
        "description": "A pressure sensor monitoring a sealed pneumatic system shows a slow, steady decrease of approximately 0.5% per hour over the past 72 hours. The rate of decrease is constant and does not vary with time of day or environmental conditions. The system was last serviced and certified leak-free 6 months ago.",
        "affected_device_types": ["pressure_sensor", "industrial_monitor"],
        "root_cause": "A developing leak in the pneumatic system at a fitting joint. The constant rate of pressure loss regardless of environmental conditions indicates a mechanical leak (as opposed to thermal contraction which would vary with temperature). The small leak rate (0.5% per hour) is consistent with a fitting that has loosened over time due to vibration — not yet a catastrophic failure but requiring attention before it worsens.",
        "resolution_steps": [
            "Calculate total pressure loss to determine current system state versus minimum operating pressure",
            "Perform ultrasonic leak detection survey on all fittings while system is pressurized",
            "Apply soap bubble testing to suspected leak points for visual confirmation",
            "Tighten or replace the leaking fitting with appropriate thread sealant",
            "Re-pressurize system and verify leak rate drops to zero (hold test for 24 hours)",
            "Implement trending alert: notify maintenance when pressure drops more than 2% from last fill within 48 hours",
            "Schedule regular fitting torque checks as part of preventive maintenance"
        ],
        "severity": "high",
        "failure_category": "mechanical_degradation",
        "tags": ["pressure", "leak", "gradual", "trending", "maintenance_required", "pneumatic"]
    },
    {
        "filename": "030_temperature_intermittent_connection.json",
        "title": "Intermittent temperature spikes from loose sensor connector causing contact resistance",
        "description": "A temperature sensor reports brief spikes of 10-20°C above baseline at irregular intervals (minutes to hours apart). Each spike lasts exactly one measurement cycle (30 seconds) and then returns to normal. Mechanical vibration from nearby equipment seems to increase spike frequency.",
        "affected_device_types": ["temperature_sensor", "industrial_monitor"],
        "root_cause": "A loose connector at the sensor junction box creates intermittent high contact resistance in the measurement circuit. When the connector's contact resistance increases (due to vibration), the additional resistance in the RTD circuit is interpreted as higher temperature. The single-cycle duration indicates the contact condition changes randomly between measurements. Vibration exacerbates the issue by physically displacing the connector contacts.",
        "resolution_steps": [
            "Inspect sensor connector and junction box for signs of loose or corroded connections",
            "Clean connector contacts with appropriate electrical contact cleaner",
            "Re-seat and secure the connector with proper torque specification",
            "If connector shows wear or corrosion, replace with new mating pair",
            "Apply vibration-resistant locking mechanism (connector retaining clip or thread lock)",
            "Verify fix by monitoring for 48 hours under normal vibration conditions",
            "Implement spike detection rule: flag readings that deviate significantly for exactly one sample then return to baseline"
        ],
        "severity": "medium",
        "failure_category": "wiring_fault",
        "tags": ["temperature", "intermittent", "connector", "vibration", "contact_resistance", "spike"]
    },
]

def main():
    output_dir = "data/incidents"
    os.makedirs(output_dir, exist_ok=True)

    # Write the generated incidents
    for incident in INCIDENTS:
        filename = incident.pop("filename")
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w") as f:
            json.dump(incident, f, indent=2)
        print(f"  Created: {filepath}")

    print(f"\nGenerated {len(INCIDENTS)} incident files in {output_dir}/")
    print("Combined with hand-crafted files (001-005), total: {0}".format(len(INCIDENTS) + 5))


if __name__ == "__main__":
    main()
    