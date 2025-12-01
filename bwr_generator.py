import csv
import math
import os
from datetime import datetime
import config

class BwrGenerator:
    def __init__(self):
        self.input_csv = os.path.join(config.RAW_DATA_DIR, "bwr_output.csv")
        self.output_dir = "dane"
        self.output_file = os.path.join(self.output_dir, "bwr.txt")
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Pressure levels from config
        self.levels = config.BWR_LEVELS_MB

    def _pressure_to_height_std(self, p_hpa):
        """
        Converts pressure (hPa) to geopotential height (meters) using Standard Atmosphere.
        """
        if p_hpa > 226.32:
            # Troposphere
            h = 44330.0 * (1.0 - (p_hpa / 1013.25)**(1.0 / 5.25588))
        else:
            # Stratosphere
            h = 11000.0 - math.log(p_hpa / 226.32) / 1.5769e-4
        return h

    def _get_wind_direction_speed(self, u, v):
        """
        Calculates meteorological wind direction (degrees) and speed (km/h) from U, V (m/s).
        """
        speed_ms = math.sqrt(u**2 + v**2)
        speed_kph = speed_ms * 3.6

        # Direction: from where the wind blows.
        angle_rad = math.atan2(v, u)
        angle_deg = math.degrees(angle_rad)
        direction = (270 - angle_deg) % 360

        return direction, speed_kph

    def _interpolate_value(self, x_target, x1, y1, x2, y2):
        """Linear interpolation."""
        if x2 == x1:
            return y1
        return y1 + (x_target - x1) * (y2 - y1) / (x2 - x1)

    def _format_dtg(self, dt):
        """Formats datetime to DDHHMMZMONYYYY"""
        return dt.strftime("%d%H%MZ%b%Y").upper()

    def generate_reports(self, exer, dtg_created_dt, start_model_dt, start_forecast_dt, end_forecast_dt):
        """
        Generates BWR reports for all areas matching the end_forecast_dt.

        Args:
            exer: String (e.g., "EXER/TESTCOAS/-")
            dtg_created_dt: datetime of report creation
            start_model_dt: datetime of model start
            start_forecast_dt: datetime of forecast start
            end_forecast_dt: datetime of forecast end (target time for data)
        """

        # Prepare header fields
        dtg_str = f"DTG/{self._format_dtg(dtg_created_dt)}//"

        zulum_str = f"ZULUM/{self._format_dtg(start_model_dt)}/{self._format_dtg(start_forecast_dt)}/{self._format_dtg(end_forecast_dt)}//"

        if not exer.endswith("//"):
            exer += "//"

        reports = []

        # Read CSV and process rows matching end_forecast_dt
        with open(self.input_csv, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)

        if len(rows) < 3:
            print(f"Warning: {self.input_csv} is empty or invalid.")
            return

        header = rows[1]
        try:
            idx_name = header.index("bwr_name")
            idx_yyyy = header.index("yyyy")
            idx_mm = header.index("mm")
            idx_dd = header.index("dd")
            idx_hh = header.index("hh")

            level_indices = {}
            for level in self.levels:
                level_indices[level] = (header.index(f"u{level}"), header.index(f"v{level}"))
        except ValueError as e:
            print(f"Error parsing CSV header: {e}")
            return

        # Target time matching (round to hour)
        target_time = end_forecast_dt.replace(minute=0, second=0, microsecond=0)

        count = 0
        for row in rows[2:]:
            try:
                r_yyyy = int(row[idx_yyyy])
                r_mm = int(row[idx_mm])
                r_dd = int(row[idx_dd])
                r_hh = int(row[idx_hh])

                row_dt = datetime(r_yyyy, r_mm, r_dd, r_hh)

                if row_dt == target_time:
                    # Found a matching row (Area)
                    area_name = row[idx_name]
                    report = self._create_single_report(row, level_indices, area_name, exer, dtg_str, zulum_str)
                    reports.append(report)
                    count += 1
            except Exception as e:
                print(f"Error processing row: {e}")
                continue

        # Write to file
        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(reports))

        print(f"Generated {count} BWR reports in {self.output_file}")

    def _create_single_report(self, row, level_indices, area_name, exer, dtg_str, zulum_str):
        # Extract and sort data points by height
        data_points = []
        for level in self.levels:
            idx_u, idx_v = level_indices[level]
            u = float(row[idx_u])
            v = float(row[idx_v])
            h = self._pressure_to_height_std(level)
            data_points.append((h, u, v))

        data_points.sort(key=lambda x: x[0])

        # Generate layers
        layer_strs = []
        for layer_top_km in range(2, 32, 2):
            layer_mid_km = layer_top_km - 1
            target_h = layer_mid_km * 1000.0

            # Interpolate
            p1 = None
            p2 = None

            if target_h < data_points[0][0]:
                p1 = data_points[0]
                p2 = data_points[0]
            elif target_h > data_points[-1][0]:
                p1 = data_points[-1]
                p2 = data_points[-1]
            else:
                for i in range(len(data_points) - 1):
                    if data_points[i][0] <= target_h <= data_points[i+1][0]:
                        p1 = data_points[i]
                        p2 = data_points[i+1]
                        break

            # Fallback
            if p1 is None:
                p1 = data_points[0]
                p2 = data_points[0]

            u = self._interpolate_value(target_h, p1[0], p1[1], p2[0], p2[1])
            v = self._interpolate_value(target_h, p1[0], p1[2], p2[0], p2[2])

            direction, speed = self._get_wind_direction_speed(u, v)

            hh_str = f"{layer_top_km:02d}"
            ddd_str = f"{int(round(direction)):03d}"
            fff_str = f"{int(round(speed)):03d}"

            layer_strs.append(f"{hh_str}/{ddd_str}/{fff_str}")

        layer_line = "LAYERM/" + "/".join(layer_strs) + "//"

        lines = []
        lines.append(exer)
        lines.append("MSGID/CBRN BWR/IMGW-PIB/-/-/-/-/-/-/-/-//")
        lines.append("GEODATUM/WGE//")
        lines.append(dtg_str)
        lines.append(f"AREAM/{area_name}//")
        lines.append(zulum_str)
        lines.append("UNITM/-/DGG/KPH/-//")
        lines.append(layer_line)

        return "\n".join(lines)

if __name__ == "__main__":
    # Test block
    gen = BwrGenerator()
    # Mock datetimes
    now = datetime(2025, 4, 14, 19, 33)
    start_model = datetime(2025, 4, 14, 12, 0)
    start_forecast = datetime(2025, 4, 16, 0, 0)
    end_forecast = datetime(2025, 4, 16, 6, 0)

    gen.generate_reports("EXER/TESTCOAS/-", now, start_model, start_forecast, end_forecast)
