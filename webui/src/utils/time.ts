import dayjs from "dayjs";

export function toDateString(value: dayjs.Dayjs): string {
  return value.format("YYYY-MM-DD");
}
