a = ""


def timeConversion(time: str) -> str:
    """Converts 12 hour time to 24 hour time
    Example: 12:00PM -> 2400
    Example: 3.00PM -> 1500
    """
    parts = time.split(":")
    lastPart = parts[1]
    firstPart = parts[0]

    frontResult = ""
    backResult = lastPart[:-2]

    if lastPart[-2:].upper() == "AM":
        frontResult = ("0" + firstPart) if len(firstPart) == 1 else firstPart
        if firstPart == "12":
            frontResult = "00"
    elif lastPart[-2:].upper() == "PM":
        frontResult = str(int(firstPart) + 12)
    else:
        print("Error: Invalid time format")
        return

    return frontResult + backResult
