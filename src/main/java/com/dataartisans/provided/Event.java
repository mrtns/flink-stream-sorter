package com.dataartisans.provided;

public class Event {
  public long time;
  public String someData;

  public Event() {}

  public Event(long time, String someData) {
    this.time = time;
    this.someData = someData;
  }

  @Override
  public String toString() {
    return String.format("Event(%d, %s)", time, someData);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Event event = (Event) o;

    if (time != event.time) return false;
    return someData != null ? someData.equals(event.someData) : event.someData == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (time ^ (time >>> 32));
    result = 31 * result + (someData != null ? someData.hashCode() : 0);
    return result;
  }
}
