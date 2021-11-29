import faust
import math
import matplotlib.pyplot as plt
import numpy as np
import matplotlib
import matplotlib.cm as cm
from datetime import datetime, timezone
from dateutil.parser import parse as parse_date
from faust.models.fields import DatetimeField

matplotlib.use('TkAgg')
plt.rcParams["figure.figsize"] = (16, 6)
number_skaters = 5


def main() -> None:
    app.main()


app = faust.App('shorttrack', broker='kafka://localhost:29092')


class Raw_Record(faust.Record, validation=True, serializer='json', coerce=True):
    time: datetime = DatetimeField(date_parser=parse_date)
    x: float
    y: float
    tracker_id: int
    camera_id: int
    skater_id: int
    lap: int
    EventTimestamp: int


class Speed_Record(faust.Record, validation=True, serializer='json', coerce=True):
    tracking_data: Raw_Record
    distance: float
    duration: float
    speed: float
    grouped_speed: float
    total_dist: float
    dist_tofinish: float
    seconds_left: float


class prob_Record(faust.Record, validation=True, serializer='json', coerce=True):
    time: datetime = DatetimeField(date_parser=parse_date)
    skater_id: float
    x: float
    y: float
    seconds_left: float
    dist_tofinish: float
    prob_percent: float
    grouped_speed: float
    lap: int


class grouped_Record(faust.Record, validation=True, serializer='json', coerce=True):
    time: datetime = DatetimeField(date_parser=parse_date)
    skater_id: float
    grouped_speed: float


tracking_topic = app.topic('simulation', value_type=Raw_Record)
diff_topic = app.topic('diff_topic', value_type=Speed_Record)
tracking_topic_proc = app.topic('full_tracking', value_type=Speed_Record)

grouped_topic = app.topic('grouped_speed', value_type=grouped_Record)

final_topic = app.topic('final', value_type=prob_Record)

default_entry = [datetime.now(timezone.utc), 0.0, 0.0]
prev_values = [default_entry] * 8


@app.agent(tracking_topic)
async def calc_speed(stream):
    async for event in stream:
        current_skater = event.skater_id
        if prev_values[current_skater - 1] == default_entry:
            prev_values[current_skater - 1] = [event.time, event.x, event.y]
            dist = math.sqrt(((0 - event.x) ** 2) + ((-11.0 - event.y) ** 2))
            first_entry = Speed_Record(distance=dist, duration=0.0, speed=0.0, total_dist=0.0, dist_tofinish=0.0,
                                       seconds_left=0.0, grouped_speed=0.0,
                                       tracking_data=Raw_Record(time=event.time, x=event.x, y=event.y,
                                                                tracker_id=event.tracker_id,
                                                                camera_id=event.camera_id, skater_id=event.skater_id,
                                                                EventTimestamp=event.EventTimestamp, lap=event.lap))
            await diff_topic.send(value=first_entry)

        else:
            # calc distance between two measuring points
            dist = math.sqrt(((prev_values[current_skater - 1][1] - event.x) ** 2) + (
                        (prev_values[current_skater - 1][2] - event.y) ** 2))

            # calc passed time between two measuring points
            diff_time = event.time - prev_values[current_skater - 1][0]
            diff_sec = float(diff_time.total_seconds())

            # calc speed between the last two measuring points
            current_speed = dist / diff_sec
            prev_values[current_skater - 1] = [event.time, event.x, event.y]

            speed_values = Speed_Record(distance=dist, duration=diff_sec, speed=current_speed, total_dist=0.0,
                                        dist_tofinish=0.0, seconds_left=0.0,
                                        grouped_speed=0.0,
                                        tracking_data=Raw_Record(time=event.time, x=event.x, y=event.y,
                                                                 tracker_id=event.tracker_id, camera_id=event.camera_id,
                                                                 skater_id=event.skater_id,
                                                                 EventTimestamp=event.EventTimestamp, lap=event.lap))
            await diff_topic.send(value=speed_values)


full_distances = [689.4558, 687.1191, 695.4141, 690.7709, 692.0590]
distances = [0.0] * number_skaters
distance_tofinish = [0.0] * number_skaters
seconds_tofinish = [0.0] * number_skaters
speeds = [[np.nan, np.nan, np.nan, np.nan, np.nan]] * number_skaters


@app.agent(diff_topic)
async def total_distance(stream):
    async for event in stream:
        current_skater = event.tracking_data.skater_id
        if event.distance <= 0:
            record = Speed_Record(distance=event.distance, duration=event.duration, speed=event.speed,
                                  total_dist=event.distance,
                                  dist_tofinish=full_distances[current_skater - 1],
                                  seconds_left=10000, grouped_speed=0.0,
                                  tracking_data=Raw_Record(time=event.tracking_data.time, x=event.tracking_data.x,
                                                           y=event.tracking_data.y,
                                                           tracker_id=event.tracking_data.tracker_id,
                                                           camera_id=event.tracking_data.camera_id,
                                                           skater_id=event.tracking_data.skater_id,
                                                           EventTimestamp=event.tracking_data.EventTimestamp,
                                                           lap=event.tracking_data.lap))
            await tracking_topic_proc.send(value=record)

        else:
            avg = 0
            distances[current_skater - 1] += event.distance
            distance_tofinish[current_skater - 1] = full_distances[current_skater - 1] - distances[current_skater - 1]

            if event.speed != 0:
                entry = speeds[current_skater - 1].copy()
                entry.pop(0)
                entry.append(event.speed)
                speeds[current_skater - 1] = entry
                avg = np.nanmean(speeds[current_skater - 1])
                seconds_tofinish[current_skater - 1] = distance_tofinish[current_skater - 1] / avg
            else:
                pass

            record = Speed_Record(distance=event.distance, duration=event.duration, speed=event.speed,
                                  total_dist=distances[current_skater - 1],
                                  dist_tofinish=distance_tofinish[current_skater - 1],
                                  seconds_left=seconds_tofinish[current_skater - 1],
                                  grouped_speed=avg,
                                  tracking_data=Raw_Record(time=event.tracking_data.time, x=event.tracking_data.x,
                                                           y=event.tracking_data.y,
                                                           tracker_id=event.tracking_data.tracker_id,
                                                           camera_id=event.tracking_data.camera_id,
                                                           skater_id=event.tracking_data.skater_id,
                                                           EventTimestamp=event.tracking_data.EventTimestamp,
                                                           lap=event.tracking_data.lap))
            await tracking_topic_proc.send(value=record)


prob_values = [0.0] * number_skaters
percentage_factor = 1.0 / number_skaters


@app.agent(tracking_topic_proc)
async def prob_calculations(stream):
    async for event in stream:
        current_skater = event.tracking_data.skater_id
        if event.seconds_left > 0:
            if 0.0 in prob_values:
                prob_values[current_skater - 1] = (event.seconds_left * event.dist_tofinish)

                record = prob_Record(time=event.tracking_data.time, skater_id=event.tracking_data.skater_id,
                                     x=event.tracking_data.x,
                                     y=event.tracking_data.y, seconds_left=event.seconds_left,
                                     dist_tofinish=event.dist_tofinish,
                                     prob_percent=percentage_factor, lap=event.tracking_data.lap,
                                     grouped_speed=event.grouped_speed)
                await final_topic.send(value=record)
            else:
                prob_values[current_skater - 1] = (event.seconds_left * event.dist_tofinish)
                sum_vals = sum(prob_values)
                prob_perc = ((percentage_factor - (prob_values[current_skater - 1] / sum_vals)) + percentage_factor)

                record = prob_Record(time=event.tracking_data.time, skater_id=event.tracking_data.skater_id,
                                     x=event.tracking_data.x,
                                     y=event.tracking_data.y, seconds_left=event.seconds_left,
                                     dist_tofinish=event.dist_tofinish,
                                     prob_percent=prob_perc, lap=event.tracking_data.lap,
                                     grouped_speed=event.grouped_speed)

                await final_topic.send(value=record)
        else:
            record = prob_Record(time=event.tracking_data.time, skater_id=event.tracking_data.skater_id,
                                 x=event.tracking_data.x,
                                 y=event.tracking_data.y, seconds_left=event.seconds_left,
                                 dist_tofinish=event.dist_tofinish,
                                 prob_percent=percentage_factor, lap=event.tracking_data.lap,
                                 grouped_speed=event.grouped_speed)
            await final_topic.send(value=record)


# plotting the race
x_vals = [15.5] * number_skaters
y_vals = [-10.0] * number_skaters
plt.ion()
fig, ax = plt.subplots()
points = ax.scatter(x_vals, y_vals, color=["red", "green", "blue", "black", "orange"], animated=True)
plt.xlim([-25, 25])
plt.ylim([-16, 20])
plt.show(block=False)
plt.pause(0.0001)
bg = fig.canvas.copy_from_bbox(fig.bbox)
fig.canvas.draw()
ax.draw_artist(points)
fig.canvas.blit(fig.bbox)

async def plot_race(x_vals, y_vals):
    fig.canvas.restore_region(bg)
    points.set_offsets(np.c_[x_vals, y_vals])
    ax.draw_artist(points)
    fig.canvas.blit(fig.bbox)
    fig.canvas.flush_events()


@app.agent(tracking_topic_proc)
async def test(stream):
    i = 0
    async for event in stream:
        current_skater = event.tracking_data.skater_id
        x_vals[current_skater - 1] = event.tracking_data.x
        y_vals[current_skater - 1] = event.tracking_data.y
        i += 1

        if i == (number_skaters * 2):
            i = 0
            await plot_race(x_vals, y_vals)


# plotting winning probability
x_vals = np.array([1])
y_vals = np.array([0.0])
x_vals2 = np.array([1])
y_vals2 = np.array([0.0])
x_vals3 = np.array([1])
y_vals3 = np.array([0.0])
x_vals4 = np.array([1])
y_vals4 = np.array([0.0])
x_vals5 = np.array([1])
y_vals5 = np.array([0.0])

plt.ion()
fig, ax = plt.subplots()
(points,) = ax.plot(x_vals, y_vals, color="red", linewidth=0.3, animated=True)
(points2,) = ax.plot(x_vals2, y_vals2, color="blue", linewidth=0.3, animated=True)
(points3,) = ax.plot(x_vals3, y_vals3, color="green", linewidth=0.3, animated=True)
(points4,) = ax.plot(x_vals4, y_vals4, color="black", linewidth=0.3, animated=True)
(points5,) = ax.plot(x_vals5, y_vals5, color="orange", linewidth=0.3, animated=True)

plt.xlim([0, 80])
plt.ylim([0.1, 0.3])
plt.show(block=False)
plt.pause(0.0000001)
bg = fig.canvas.copy_from_bbox(fig.bbox)
fig.canvas.draw()
ax.draw_artist(points)
ax.draw_artist(points2)
ax.draw_artist(points3)
ax.draw_artist(points4)
ax.draw_artist(points5)
fig.canvas.blit(fig.bbox)


async def plot_race(x_vals, y_vals, x_vals2, y_vals2, x_vals3, y_vals3, x_vals4, y_vals4, x_vals5, y_vals5):
    fig.canvas.restore_region(bg)
    points.set_ydata(y_vals)
    points.set_xdata(x_vals)
    points2.set_xdata(x_vals2)
    points2.set_ydata(y_vals2)
    points3.set_ydata(y_vals3)
    points3.set_xdata(x_vals3)
    points4.set_xdata(x_vals4)
    points4.set_ydata(y_vals4)
    points5.set_ydata(y_vals5)
    points5.set_xdata(x_vals5)
    ax.draw_artist(points)
    ax.draw_artist(points2)
    ax.draw_artist(points3)
    ax.draw_artist(points4)
    ax.draw_artist(points5)
    fig.canvas.blit(fig.bbox)
    fig.canvas.flush_events()


@app.agent(final_topic)
async def test(stream):
    p = 1
    p2 = 1
    p3 = 1
    p4 = 1
    p5 = 1
    x_vals = np.array([1])
    y_vals = np.array([0.0])
    x_vals2 = np.array([1])
    y_vals2 = np.array([0.0])
    x_vals3 = np.array([1])
    y_vals3 = np.array([0.0])
    x_vals4 = np.array([1])
    y_vals4 = np.array([0.0])
    x_vals5 = np.array([1])
    y_vals5 = np.array([0.0])
    async for event in stream:
        current_skater = event.skater_id

        if current_skater == 1:
            p += 1
            y_vals = np.append(y_vals, event.prob_percent)
            x_vals = np.append(x_vals, p)
        elif current_skater == 2:
            p2 += 1
            y_vals2 = np.append(y_vals2, event.prob_percent)
            x_vals2 = np.append(x_vals2, p2)
        elif current_skater == 3:
            p3 += 1
            y_vals3 = np.append(y_vals3, event.prob_percent)
            x_vals3 = np.append(x_vals3, p3)
        elif current_skater == 4:
            p4 += 1
            y_vals4 = np.append(y_vals4, event.prob_percent)
            x_vals4 = np.append(x_vals4, p4)
        elif current_skater == 5:
            p5 += 1
            y_vals5 = np.append(y_vals5, event.prob_percent)
            x_vals5 = np.append(x_vals5, p5)

        if current_skater == 1:
            await plot_race(x_vals, y_vals, x_vals2, y_vals2, x_vals3, y_vals3, x_vals4, y_vals4, x_vals5, y_vals5)


