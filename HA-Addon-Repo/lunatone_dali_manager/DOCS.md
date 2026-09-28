# Lunatone DALI-2 IoT Manager

Verwaltungsoberfläche für das **Lunatone DALI-2 IoT** Gateway direkt in Home Assistant.

## Funktionen

- **Übersicht** – Status, Fehler, Live-Busaktivität, Schnellsteuerung, letzte Ereignisse
- **Geräte** – Steuerung (Helligkeit, Farbtemperatur, Farbe), Name, Gruppen, Fadezeit/-rate, Szenen 0–15, DALI-Parameter (Min/Max/Einschalt-/Notlevel), Identifizieren
- **Gruppen & Zonen** – Gruppen-Matrix (Geräte × 16 Gruppen), Zonen anlegen/bearbeiten
- **Sensoren & Taster** – Gateway-Sensoren mit Verlauf, automatisch gelernte DALI-2-Eingänge
- **Automationen** – Sequenzen, Zeitpläne (inkl. Sonnenzeiten), Circadian-Verläufe, Trigger-Aktionen, Event-Weiterleitung, Statusabfragen
- **DALI-Bus** – Live-Busmonitor mit Dekodierung nach IEC 62386, CSV-Export, Scan, Linienstatus, Rohbefehle (Expertenmodus)
- **Analysen** – Gesundheitsbericht, Buslast, Kommunikationsqualität je Adresse, Einschaltdauer & Schaltzyklen, Energie & Diagnose, Ereignisprotokoll
- **System** – Name, Datum/Zeitzone, Standort, Einstellungen, Sicherung & Wiederherstellung, Neustart
- **Expertenbereich** – Netzwerk, Firmware-Update, DALI-Makros, Löschen, Werksreset (mit doppelter Bestätigung)

## Konfiguration

| Option | Beschreibung |
|--------|--------------|
| `gateway_host` | IP/Hostname des Gateways. Leer = automatische Suche im lokalen Netz. |
| `history_days` | Aufbewahrung von Verlauf, Statistiken und Messwerten (Standard 30 Tage). |
| `monitor_buffer` | Anzahl gespeicherter Busframes für den Busmonitor (Standard 100 000). |
| `log_level` | Log-Ausführlichkeit. |

Nach dem Verbinden meldet das Add-on das Gateway an Home Assistant – die Integration **Lunatone DALI-2 IoT** erscheint dann unter *Geräte & Dienste* als „Entdeckt“ (die Integration muss dafür nach `/config/custom_components/lunatone_dali` kopiert sein).

## Daten

Alle Verlaufsdaten liegen in `/data/lunatone.db` (SQLite) und sind in HA-Backups des Add-ons enthalten.
