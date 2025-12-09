def calculate_severity(event, otx, misp_data):
    base = event.get("initial_severity", "INFO")
    base_score = {"INFO": 1, "WARN": 2, "ERROR": 3, "CRITICAL": 4}.get(base, 1)

    score = base_score

    # OTX pulses = strong indicator
    if otx:
        pulse_count = len(otx.get("pulse_info", {}).get("pulses", []))
        if pulse_count > 0:
            score += 2

    # MISP hit = strong indicator
    if misp_data and len(misp_data.get("Attribute", [])) > 0:
        score += 2

    # Convert score → severity
    if score >= 6:
        sev = "CRITICAL"
    elif score >= 4:
        sev = "ERROR"
    elif score >= 2:
        sev = "WARN"
    else:
        sev = "INFO"

    confidence = min(score / 7, 1.0)

    return sev, confidence
