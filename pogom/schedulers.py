#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Schedulers determine how worker's queues get filled. They control which locations get scanned,
in what order, at what time. This allows further optimizations to be easily added, without
having to modify the existing overseer and worker thread code.

Schedulers will recieve:

queues - A list of queues for the workers they control. For now, this is a list containing a
            single queue.
status - A list of status dicts for the workers. Schedulers can use this information to make
            more intelligent scheduling decisions. Useful values include:
            - last_scan_date: unix timestamp of when the last scan was completed
            - location: [lat,lng,alt] of the last scan
args - The configuration arguments. This may not include all of the arguments, just ones that are
            relevant to this scheduler instance (eg. if multiple locations become supported, the args
            passed to the scheduler will only contain the parameters for the location it handles)

Schedulers must fill the queues with items to search.

Queue items are a list containing:
    [step, (latitude, longitude, altitude), appears_seconds, disappears_seconds)]
Where:
    - step is the step number. Used only for display purposes.
    - (latitude, longitude, altitude) is the location to be scanned.
    - appears_seconds is the unix timestamp of when the pokemon next appears
    - disappears_seconds is the unix timestamp of when the pokemon next disappears

    appears_seconds and disappears_seconds are used to skip scans that are too late, and wait for scans the
    worker is early for.  If a scheduler doesn't have a specific time a location needs to be scanned, it
    should set both to 0.

If implementing a new scheduler, place it before SchedulerFactory, and add it to __scheduler_classes
'''

import logging
import math
import geopy
import json
import random
import requests
import time
import sys
from copy import deepcopy
import traceback
from collections import Counter
from queue import Empty
from operator import itemgetter
from datetime import datetime, timedelta
from .transform import get_new_coords
from .models import hex_bounds, Pokemon, SpawnPoint, ScannedLocation, ScanSpawnPoint
from .utils import now, cur_sec, cellid, date_secs, equi_rect_distance

log = logging.getLogger(__name__)

# Simple base class that all other schedulers inherit from.
# Most of these functions should be overridden in the actual scheduler classes.
# Not all scheduler methods will need to use all of the functions.


class BaseScheduler(object):
    def __init__(self, queues, status, args):
        self.queues = queues
        self.status = status
        self.args = args
        self.scan_location = False
        self.size = None
        self.ready = False

    # Schedule function fills the queues with data.
    def schedule(self):
        log.warning('BaseScheduler does not schedule any items')

    # location_changed function is called whenever the location being scanned changes.
    # scan_location = (lat, lng, alt)
    def location_changed(self, scan_location, dbq):
        self.scan_location = scan_location
        self.empty_queues()

    # scanning_pause function is called when scanning is paused from the UI.
    # The default function will empty all the queues.
    # Note: This function is called repeatedly while scanning is paused!
    def scanning_paused(self):
        self.empty_queues()

    def getsize(self):
        return self.size

    def get_overseer_message(self):
        nextitem = self.queues[0].queue[0]
        message = 'Processing search queue, next item is {:6f},{:6f}'.format(nextitem[1][0], nextitem[1][1])
        # If times are specified, print the time of the next queue item, and how many seconds ahead/behind realtime
        if nextitem[2]:
            message += ' @ {}'.format(time.strftime('%H:%M:%S', time.localtime(nextitem[2])))
            if nextitem[2] > now():
                message += ' ({}s ahead)'.format(nextitem[2] - now())
            else:
                message += ' ({}s behind)'.format(now() - nextitem[2])
        return message

    # check if time to refresh queue
    def time_to_refresh_queue(self):
        return self.queues[0].empty()

    def task_done(self, *args):
        return self.queues[0].task_done()

    # Return the next item in the queue
    def next_item(self, search_items_queue):
        step, step_location, appears, leaves = self.queues[0].get()
        remain = appears - now() + 10
        messages = {
            'wait': 'Waiting for item from queue',
            'early': 'Early for {:6f},{:6f}; waiting {}s...'.format(step_location[0], step_location[1], remain),
            'late': 'Too late for location {:6f},{:6f}; skipping'.format(step_location[0], step_location[1]),
            'search': 'Searching at {:6f},{:6f}'.format(step_location[0], step_location[1]),
            'invalid': 'Invalid response at {:6f},{:6f}, abandoning location'.format(step_location[0], step_location[1])
        }
        return step, step_location, appears, leaves, messages

    # How long to delay since last action
    def delay(self, *args):
        return self.args.scan_delay  # always scan delay time

    # Function to empty all queues in the queues list
    def empty_queues(self):
        self.ready = False
        for queue in self.queues:
            if not queue.empty():
                try:
                    while True:
                        queue.get_nowait()
                except Empty:
                    pass


# Hex Search is the classic search method, with the pokepath modification, searching in a hex grid around the center location.
class HexSearch(BaseScheduler):
    elevation = False
    altitude = 0

    # Call base initialization, set step_distance.
    def __init__(self, queues, status, args):
        BaseScheduler.__init__(self, queues, status, args)

        # If we are only scanning for pokestops/gyms, the scan radius can be 900m.  Otherwise 70m.
        if self.args.no_pokemon:
            self.step_distance = 0.900
        else:
            self.step_distance = 0.070

        self.step_limit = args.step_limit
        self.gmaps = args.gmaps_key
        self.altitude_range = args.altitude_range
        self.altitude_default = args.altitude
        # This will hold the list of locations to scan so it can be reused, instead of recalculating on each loop.
        self.locations = False

    # On location change, empty the current queue and the locations list
    def location_changed(self, scan_location, dbq):
        self.scan_location = scan_location
        self.empty_queues()
        self.locations = False

    # Generates the list of locations to scan.
    def _generate_locations(self):
        NORTH = 0
        EAST = 90
        SOUTH = 180
        WEST = 270

        xdist = math.sqrt(3) * self.step_distance  # Dist between column centers.
        ydist = 3 * (self.step_distance / 2)       # Dist between row centers.

        results = []

        results.append((self.scan_location[0], self.scan_location[1], 0))

        if self.step_limit > 1:
            loc = self.scan_location

            # Upper part.
            ring = 1
            while ring < self.step_limit:

                loc = get_new_coords(loc, xdist, WEST if ring % 2 == 1 else EAST)
                results.append((loc[0], loc[1], 0))

                for i in range(ring):
                    loc = get_new_coords(loc, ydist, NORTH)
                    loc = get_new_coords(loc, xdist / 2, EAST if ring % 2 == 1 else WEST)
                    results.append((loc[0], loc[1], 0))

                for i in range(ring):
                    loc = get_new_coords(loc, xdist, EAST if ring % 2 == 1 else WEST)
                    results.append((loc[0], loc[1], 0))

                for i in range(ring):
                    loc = get_new_coords(loc, ydist, SOUTH)
                    loc = get_new_coords(loc, xdist / 2, EAST if ring % 2 == 1 else WEST)
                    results.append((loc[0], loc[1], 0))

                ring += 1

            # Lower part.
            ring = self.step_limit - 1

            loc = get_new_coords(loc, ydist, SOUTH)
            loc = get_new_coords(loc, xdist / 2, WEST if ring % 2 == 1 else EAST)
            results.append((loc[0], loc[1], 0))

            while ring > 0:

                if ring == 1:
                    loc = get_new_coords(loc, xdist, WEST)
                    results.append((loc[0], loc[1], 0))

                else:
                    for i in range(ring - 1):
                        loc = get_new_coords(loc, ydist, SOUTH)
                        loc = get_new_coords(loc, xdist / 2, WEST if ring % 2 == 1 else EAST)
                        results.append((loc[0], loc[1], 0))

                    for i in range(ring):
                        loc = get_new_coords(loc, xdist, WEST if ring % 2 == 1 else EAST)
                        results.append((loc[0], loc[1], 0))

                    for i in range(ring - 1):
                        loc = get_new_coords(loc, ydist, NORTH)
                        loc = get_new_coords(loc, xdist / 2, WEST if ring % 2 == 1 else EAST)
                        results.append((loc[0], loc[1], 0))

                    loc = get_new_coords(loc, xdist, EAST if ring % 2 == 1 else WEST)
                    results.append((loc[0], loc[1], 0))

                ring -= 1

        # This will pull the last few steps back to the front of the list,
        # so you get a "center nugget" at the beginning of the scan, instead
        # of the entire nothern area before the scan spots 70m to the south.
        if self.step_limit >= 3:
            if self.step_limit == 3:
                results = results[-2:] + results[:-2]
            else:
                results = results[-7:] + results[:-7]

        # Add the required appear and disappear times.
        locationsZeroed = []
        for step, location in enumerate(results, 1):
            if HexSearch.elevation:
                altitude = HexSearch.altitude
            else:
                try:
                    r_session = requests.Session()
                    response = r_session.get("https://maps.googleapis.com/maps/api/elevation/json?locations={},{}&key={}".format(location[0], location[1], self.gmaps))
                    response = response.json()
                    altitude = response["results"][0]["elevation"]
                    HexSearch.elevation = True
                    HexSearch.altitude = altitude
                except:
                    altitude = self.altitude_default
            if self.altitude_range > 0:
                altitude = altitude + random.randrange(-1 * self.altitude_range, self.altitude_range) + float(format(random.random(), '.13f'))
            else:
                altitude = altitude + float(format(random.random(), '.13f'))

            locationsZeroed.append((step, (location[0], location[1], altitude), 0, 0))
        return locationsZeroed

    # Schedule the work to be done.
    def schedule(self):
        if not self.scan_location:
            log.warning('Cannot schedule work until scan location has been set')
            return

        # Only generate the list of locations if we don't have it already calculated.
        if not self.locations:
            self.locations = self._generate_locations()

        for location in self.locations:
            # FUTURE IMPROVEMENT - For now, queues is assumed to have a single queue.
            self.queues[0].put(location)
            log.debug("Added location {}".format(location))
        self.size = len(self.locations)
        self.ready = True


# Spawn Only Hex Search works like Hex Search, but skips locations that have no known spawnpoints.
class HexSearchSpawnpoint(HexSearch):

    def _any_spawnpoints_in_range(self, coords, spawnpoints):
        return any(geopy.distance.distance(coords, x).meters <= 70 for x in spawnpoints)

    # Extend the generate_locations function to remove locations with no spawnpoints.
    def _generate_locations(self):
        n, e, s, w = hex_bounds(self.scan_location, self.step_limit)
        spawnpoints = set((d['latitude'], d['longitude']) for d in Pokemon.get_spawnpoints(s, w, n, e))

        if len(spawnpoints) == 0:
            log.warning('No spawnpoints found in the specified area!  (Did you forget to run a normal scan in this area first?)')

        # Call the original _generate_locations.
        locations = super(HexSearchSpawnpoint, self)._generate_locations()

        # Remove items with no spawnpoints in range.
        locations = [coords for coords in locations if self._any_spawnpoints_in_range(coords[1], spawnpoints)]
        return locations


# Spawn Scan searches known spawnpoints at the specific time they spawn.
class SpawnScan(BaseScheduler):
    elevation = False
    altitude = 0

    def __init__(self, queues, status, args):
        BaseScheduler.__init__(self, queues, status, args)
        # On the first scan, we want to search the last 15 minutes worth of spawns to get existing
        # pokemon onto the map.
        self.firstscan = True

        # If we are only scanning for pokestops/gyms, the scan radius can be 900m.  Otherwise 70m.
        if self.args.no_pokemon:
            self.step_distance = 0.900
        else:
            self.step_distance = 0.070

        self.step_limit = args.step_limit
        self.locations = False
        self.gmaps = args.gmaps_key
        self.altitude_range = args.altitude_range
        self.altitude_default = args.altitude
    # Generate locations is called when the locations list is cleared - the first time it scans or after a location change.

    def _generate_locations(self):
        # Attempt to load spawns from file.
        if self.args.spawnpoint_scanning != 'nofile':
            log.debug('Loading spawn points from json file @ %s', self.args.spawnpoint_scanning)
            try:
                with open(self.args.spawnpoint_scanning) as file:
                    self.locations = json.load(file)
            except ValueError as e:
                log.exception(e)
                log.error('JSON error: %s; will fallback to database', e)
            except IOError as e:
                log.error('Error opening json file: %s; will fallback to database', e)

        # No locations yet? Try the database!
        if not self.locations:
            log.debug('Loading spawn points from database')
            self.locations = Pokemon.get_spawnpoints_in_hex(self.scan_location, self.args.step_limit)

        # Well shit...
        # if not self.locations:
        #    raise Exception('No availabe spawn points!')

        # locations[]:
        # {"lat": 37.53079079414139, "lng": -122.28811690874117, "spawnpoint_id": "808f9f1601d", "time": 511

        log.info('Total of %d spawns to track', len(self.locations))

        # locations.sort(key=itemgetter('time'))

        if self.args.very_verbose:
            for i in self.locations:
                sec = i['time'] % 60
                minute = (i['time'] / 60) % 60
                m = 'Scan [{:02}:{:02}] ({}) @ {},{}'.format(minute, sec, i['time'], i['lat'], i['lng'])
                log.debug(m)

        # 'time' from json and db alike has been munged to appearance time as seconds after the hour.
        # Here we'll convert that to a real timestamp.
        for location in self.locations:
            # For a scan which should cover all CURRENT pokemon, we can offset
            # the comparison time by 15 minutes so that the "appears" time
            # won't be rolled over to the next hour.

            # TODO: Make it work. The original logic (commented out) was producing
            #       bogus results if your first scan was in the last 15 minute of
            #       the hour. Wrapping my head around this isn't work right now,
            #       so I'll just drop the feature for the time being. It does need
            #       to come back so that repositioning/pausing works more nicely,
            #       but we can live without it too.

            # if sps_scan_current:
            #     cursec = (location['time'] + 900) % 3600
            # else:
            cursec = location['time']

            if cursec > cur_sec():
                # Hasn't spawn in the current hour.
                from_now = location['time'] - cur_sec()
                appears = now() + from_now
            else:
                # Won't spawn till next hour.
                late_by = cur_sec() - location['time']
                appears = now() + 3600 - late_by

            location['appears'] = appears
            location['leaves'] = appears + 900

        # Put the spawn points in order of next appearance time.
        self.locations.sort(key=itemgetter('appears'))

        # Match expected structure:
        # locations = [((lat, lng, alt), ts_appears, ts_leaves),...]
        retset = []
        for step, location in enumerate(self.locations, 1):
            if SpawnScan.elevation:
                altitude = SpawnScan.altitude
            else:
                try:
                    r_session = requests.Session()
                    response = r_session.get("https://maps.googleapis.com/maps/api/elevation/json?locations={},{}&key={}".format(location['lat'], location['lng'], self.gmaps))
                    response = response.json()
                    altitude = response["results"][0]["elevation"]
                    SpawnScan.elevation = True
                    SpawnScan.altitude = altitude
                except:
                    altitude = self.altitude_default
            if self.altitude_range > 0:
                altitude = altitude + random.randrange(-1 * self.altitude_range, self.altitude_range) + float(format(random.random(), '.13f'))
            else:
                altitude = altitude + float(format(random.random(), '.13f'))
            retset.append((step, (location['lat'], location['lng'], altitude), location['appears'], location['leaves']))

        return retset

    # Schedule the work to be done.
    def schedule(self):
        if not self.scan_location:
            log.warning('Cannot schedule work until scan location has been set')
            return

        # SpawnScan needs to calculate the list every time, since the times will change.
        self.locations = self._generate_locations()

        for location in self.locations:
            # FUTURE IMPROVEMENT - For now, queues is assumed to have a single queue.
            self.queues[0].put(location)
            log.debug("Added location {}".format(location))

        # Clear the locations list so it gets regenerated next cycle.
        self.size = len(self.locations)
        self.locations = None
        self.ready = True


# SpeedScan is a complete search method that initially does a spawnpoint search in each scan location by scanning
# five two-minute bands within an hour and ten minute intervals between bands.

# After finishing the spawnpoint search or if timing isn't right for any of the remaining search bands,
# workers will search the nearest scan location that has a new spawn.
class SpeedScan(HexSearch):

    # Call base initialization, set step_distance
    def __init__(self, queues, status, args):
        super(SpeedScan, self).__init__(queues, status, args)
        self.refresh_date = datetime.utcnow() - timedelta(days=1)
        self.next_band_date = self.refresh_date
        self.queues = [[]]
        self.ready = False
        self.spawns_found = 0
        self.spawns_missed_delay = {}
        self.scans_done = 0
        self.scans_missed = 0
        self.scans_missed_list = []
        self.minutes = 5  # Minutes between queue refreshes. Should be less than 10 to allow for new bands during Initial scan
        self.found_percent = []
        self.scan_percent = []
        self.spawn_percent = []
        self.status_message = []
        self._stat_init()

    def _stat_init(self):
        self.spawns_found = 0
        self.spawns_missed_delay = {}
        self.scans_done = 0
        self.scans_missed = 0
        self.scans_missed_list = []

    # On location change, empty the current queue and the locations list
    def location_changed(self, scan_location, db_update_queue):
        super(SpeedScan, self).location_changed(scan_location, db_update_queue)
        self.locations = self._generate_locations()
        scans = {}
        initial = {}
        all_scans = {}
        for sl in ScannedLocation.select_in_hex(self.scan_location, self.args.step_limit):
            all_scans[cellid((sl['latitude'], sl['longitude']))] = sl

        for i, e in enumerate(self.locations):
            cell = cellid(e[1])
            scans[cell] = {'loc': e[1],  # Lat/long pair
                           'step': e[0]}

            initial[cell] = all_scans[cell] if cell in all_scans.keys() else ScannedLocation.new_loc(e[1])

        self.scans = scans
        db_update_queue.put((ScannedLocation, initial))
        log.info('%d steps created', len(scans))
        self.band_spacing = int(10 * 60 / len(scans))
        self.band_status()
        spawnpoints = SpawnPoint.select_in_hex(self.scan_location, self.args.step_limit)
        if not spawnpoints:
            log.info('No spawnpoints in hex found in SpawnPoint table. Doing initial scan.')
        log.info('Found %d spawn points within hex', len(spawnpoints))

        log.info('Doing %s distance calcs to assign %d spawn points to %d scans',
                 "{:,}".format(len(spawnpoints) * len(scans)), len(spawnpoints), len(scans))
        log.info('Until the intial scan is complete, this step can take a long time for large -st.')
        log.info('If wait is too long, reduce the -st size and complete intial scan before increasing -st again.')
        scan_spawn_point = {}
        ScannedLocation.link_spawn_points(scans, initial, spawnpoints, self.step_distance, scan_spawn_point)
        if len(scan_spawn_point):
            log.info('%d relations found between the spawn points and steps', len(scan_spawn_point))
            db_update_queue.put((ScanSpawnPoint, scan_spawn_point))
        else:
            log.info('Spawn points assigned')

    # Generates the list of locations to scan
    # Created a new function, because speed scan requires fixed locations, even when increasing
    # -st. With HexSearch locations, the location of inner rings would change if -st was increased
    # requiring rescanning since it didn't recognize the location in the ScannedLocation table
    def _generate_locations(self):

        NORTH = 0
        EAST = 90
        SOUTH = 180
        WEST = 270

        xdist = math.sqrt(3) * self.step_distance  # dist between column centers
        ydist = 3 * (self.step_distance / 2)       # dist between row centers

        results = []

        loc = self.scan_location
        results.append((loc[0], loc[1], 0))

        # upper part
        for ring in range(1, self.step_limit):

            for i in range(max(ring - 1, 1)):
                if ring > 1:
                    loc = get_new_coords(loc, ydist, NORTH)

                loc = get_new_coords(loc, xdist / (1 + (ring > 1)), WEST)
                results.append((loc[0], loc[1], 0))

            for i in range(ring):
                loc = get_new_coords(loc, ydist, NORTH)
                loc = get_new_coords(loc, xdist / 2, EAST)
                results.append((loc[0], loc[1], 0))

            for i in range(ring):
                loc = get_new_coords(loc, xdist, EAST)
                results.append((loc[0], loc[1], 0))

            for i in range(ring):
                loc = get_new_coords(loc, ydist, SOUTH)
                loc = get_new_coords(loc, xdist / 2, EAST)
                results.append((loc[0], loc[1], 0))

            for i in range(ring):
                loc = get_new_coords(loc, ydist, SOUTH)
                loc = get_new_coords(loc, xdist / 2, WEST)
                results.append((loc[0], loc[1], 0))

            for i in range(ring + (ring + 1 < self.step_limit)):
                loc = get_new_coords(loc, xdist, WEST)
                results.append((loc[0], loc[1], 0))

        return [(step, (location[0], location[1], 0), 0, 0) for step, location in enumerate(results)]

    def getsize(self):
        return len(self.queues[0])

    def get_overseer_message(self):
        n = 0
        ms = (datetime.utcnow() - self.refresh_date).total_seconds() + self.refresh_ms
        counter = {
            'TTH': 0,
            'spawn': 0,
            'band': 0,
        }
        for item in self.queues[0]:
            if item.get('done', False):
                continue

            if ms > item['end']:
                continue

            if ms < item['start']:
                break

            n += 1
            counter[item['kind']] += 1

        message = 'Scanning status: {} total waiting, {} initial bands, {} TTH searches, and {} new spawns'\
            .format(n, counter['band'], counter['TTH'], counter['spawn'])
        if self.status_message:
            message += '\n' + self.status_message

        return message

    # Refresh queue every 5 minutes
    # the first band of a scan is done
    def time_to_refresh_queue(self):
        return (datetime.utcnow() - self.refresh_date).total_seconds() > self.minutes * 60 or \
            self.queues == [[]]

    # Function to empty all queues in the queues list
    def empty_queues(self):
        self.queues = [[]]

    # How long to delay since last action
    def delay(self, last_scan_date):
        return max((last_scan_date - datetime.utcnow()).total_seconds() + self.args.scan_delay, 2)

    def band_status(self):
        try:
            bands_total = len(self.locations) * 5
            bands_filled = ScannedLocation.bands_filled(self.locations)
            percent = bands_filled * 100.0 / bands_total
            if bands_total == bands_filled:
                log.info('Initial spawnpoint scan is complete')
            else:
                log.info('Initial spawnpoint scan, %d of %d bands are done or %.1f%% complete',
                         bands_filled, bands_total, percent)
            return percent

        except Exception as e:
            log.error('Exception in band_status: Exception message: {}'.format(e))

    # Update the queue, and provide a report on performance of last minutes
    def schedule(self):
        log.info('Refreshing queue')
        self.ready = False
        now_date = datetime.utcnow()
        self.refresh_date = now_date
        self.refresh_ms = now_date.minute * 60 + now_date.second
        old_q = deepcopy(self.queues[0])
        queue = []

        for cell, scan in self.scans.iteritems():
            queue += ScannedLocation.get_times(scan, now_date)
            queue += SpawnPoint.get_times(cell, scan, now_date, self.args.spawn_delay)

        queue.sort(key=itemgetter('start'))
        self.queues[0] = queue
        self.ready = True
        log.info('New queue created with %d entries', len(queue))
        if len(old_q):
            try:  # Enclosing in try: to avoid divide by zero exceptions from killing overseer

                # Possible 'done' values are 'Missed', 'Scanned', None, or number
                Not_none_list = filter(lambda e: e.get('done', None) is not None, old_q)
                Missed_list = filter(lambda e: e.get('done', None) == 'Missed', Not_none_list)
                Scanned_list = filter(lambda e: e.get('done', None) == 'Scanned', Not_none_list)
                Timed_list = filter(lambda e: type(e['done']) is not str, Not_none_list)
                spawns_timed_list = filter(lambda e: e['kind'] == 'spawn', Timed_list)
                spawns_timed = len(spawns_timed_list)
                bands_timed = len(filter(lambda e: e['kind'] == 'band', Timed_list))
                spawns_all = spawns_timed + len(filter(lambda e: e['kind'] == 'spawn', Scanned_list))
                spawns_missed = len(filter(lambda e: e['kind'] == 'spawn', Missed_list))
                band_percent = self.band_status()
                kinds = {}
                tth_ranges = {}
                tth_found = 0
                active_sp = 0
                found_percent = 100.0
                good_percent = 100.0
                spawns_reached = 100.0
                spawnpoints = SpawnPoint.select_in_hex(self.scan_location, self.args.step_limit)
                for sp in spawnpoints:
                    if sp['missed_count'] > 5:
                        continue
                    active_sp += 1
                    tth_found += sp['earliest_unseen'] == sp['latest_seen']
                    kind = sp['kind']
                    kinds[kind] = kinds.get(kind, 0) + 1
                    tth_range = str(int(round(((sp['earliest_unseen'] - sp['latest_seen']) % 3600) / 60.0)))
                    tth_ranges[tth_range] = tth_ranges.get(tth_range, 0) + 1

                tth_ranges['0'] = tth_ranges.get('0', 0) - tth_found
                len_spawnpoints = len(spawnpoints) + (not len(spawnpoints))
                log.info('Total Spawn Points found in hex: %d', len(spawnpoints))
                log.info('Inactive Spawn Points found in hex: %d or %.1f%%',
                         len(spawnpoints) - active_sp, (len(spawnpoints) - active_sp) * 100.0 / len_spawnpoints)
                log.info('Active Spawn Points found in hex: %d or %.1f%%',
                         active_sp, active_sp * 100.0 / len_spawnpoints)
                active_sp += active_sp == 0
                for k in sorted(kinds.keys()):
                    log.info('%s kind spawns: %d or %.1f%%', k, kinds[k], kinds[k] * 100.0 / active_sp)
                log.info('Spawns with found TTH: %d or %.1f%%', tth_found, tth_found * 100.0 / active_sp)
                for k in sorted(tth_ranges.keys(), key=int):
                    log.info('Spawnpoints with a %sm range to find TTH: %d', k, tth_ranges[k])
                log.info('Over last %d minutes: %d new bands, %d Pokemon found',
                         self.minutes, bands_timed, spawns_all)
                log.info('Of the %d total spawns, %d were targeted, and %d found scanning for others',
                         spawns_all, spawns_timed, spawns_all - spawns_timed)
                scan_total = spawns_timed + bands_timed
                spm = scan_total / self.minutes
                seconds_per_scan = self.minutes * 60 * self.args.workers / scan_total if scan_total else 0
                log.info('%d scans over %d minutes, %d scans per minute, %d secs per scan per worker',
                         scan_total, self.minutes, spm, seconds_per_scan)

                sum = spawns_all + spawns_missed
                if sum:
                    spawns_reached = spawns_all * 100.0 / (spawns_all + spawns_missed)
                    log.info('%d Pokemon found, and %d were not reached in time for %.1f%% found',
                             spawns_all, spawns_missed, spawns_reached)

                if spawns_timed:
                    average = reduce(lambda x, y: x + y['done'], spawns_timed_list, 0) / spawns_timed
                    log.info('%d Pokemon found, %d were targeted, with an average delay of %d sec',
                             spawns_all, spawns_timed, average)

                    spawns_missed = reduce(lambda x, y: x + len(y), self.spawns_missed_delay.values(), 0)
                    sum = spawns_missed + self.spawns_found
                    found_percent = self.spawns_found * 100.0 / sum if sum else 0
                    log.info('%d spawns scanned and %d spawns were not there when expected for %.1f%%',
                             self.spawns_found, spawns_missed, found_percent)
                    self.spawn_percent.append(round(found_percent, 1))
                    if self.spawns_missed_delay:
                        log.warning('Missed spawn IDs with times after spawn:')
                        log.warning(self.spawns_missed_delay)
                    log.info('History: %s', str(self.spawn_percent).strip('[]'))

                sum = self.scans_done + len(self.scans_missed_list)
                good_percent = self.scans_done * 100.0 / sum if sum else 0
                log.info('%d scans successful and %d scans missed for %.1f%% found',
                         self.scans_done, len(self.scans_missed_list), good_percent)
                self.scan_percent.append(round(good_percent, 1))
                if self.scans_missed_list:
                    log.warning('Missed scans: %s', Counter(self.scans_missed_list).most_common(3))
                    log.info('History: %s', str(self.scan_percent).strip('[]'))
                self.status_message = 'Initial scan: {:.2f}%, TTH found: {:.2f}%, '.format(band_percent, tth_found * 100.0 / active_sp)
                self.status_message += 'Spawns reached: {:.2f}%, Spawns found: {:.2f}%, Good scans {:.2f}%'\
                    .format(spawns_reached, found_percent, good_percent)
                self._stat_init()

            except Exception as e:
                log.error('Performance statistics had an Exception: {}'.format(e))
                traceback.print_exc(file=sys.stdout)

    # Find the best item to scan next
    def next_item(self, status):
        # score each item in the queue by # of due spawns or scan time bands can be filled

        while not self.ready:
            time.sleep(1)

        now_date = datetime.utcnow()
        now_time = time.time()
        n = 0  # count valid scans reviewed
        q = self.queues[0]
        ms = (now_date - self.refresh_date).total_seconds() + self.refresh_ms
        best = {'score': 0}
        cant_reach = False
        worker_loc = [status['latitude'], status['longitude']]
        last_action = status['last_scan_date']

        # check all scan locations possible in the queue
        for i, item in enumerate(q):
            # if already claimed by another worker or done, pass
            if item.get('done', False):
                continue

            # if already timed out, mark it as Missed and check next
            if ms > item['end']:
                item['done'] = 'Missed' if not item.get('done', False) else item['done']
                continue

            # if we just did a fresh band recently, wait a few seconds to space out the band scans
            if now_date < self.next_band_date:
                continue

            # if the start time isn't yet, don't bother looking further, since queue sorted by start time
            if ms < item['start']:
                break

            loc = item['loc']
            distance = equi_rect_distance(loc, worker_loc)
            secs_to_arrival = distance / self.args.kph * 3600

            # if we can't make it there before it disappears, don't bother trying
            if ms + secs_to_arrival > item['end']:
                cant_reach = True
                continue

            n += 1

            # Bands are top priority to find new spawns first
            score = 1e12 if item['kind'] == 'band' else (1e6 if item['kind'] == 'TTH' else 1)

            # For spawns, score is purely based on how close they are to last worker position
            score = score / (distance + .01)

            if score > best['score']:
                best = {'score': score, 'i': i}
                best.update(item)

        prefix = 'Calc %.2f for %d scans:' % (time.time() - now_time, n)
        loc = best.get('loc', [])
        step = best.get('step', 0)
        i = best.get('i', 0)
        messages = {
            'wait': 'Nothing to scan',
            'early': 'Early for step {}; waiting a few seconds...'.format(step),
            'late': 'API response on step {} delayed by {} seconds. Possible causes: slow proxies, internet, or Niantic servers'.format(step, int((now_date - last_action).total_seconds())),
            'search': 'Searching at step {}'.format(step),
            'invalid': 'Invalid response at step {}, abandoning location'.format(step)
        }

        try:
            item = q[i]
        except IndexError:
            messages['wait'] = 'Search aborting. Overseer refreshing queue.'
            return -1, 0, 0, 0, messages

        if best['score'] == 0:
            if cant_reach:
                messages['wait'] = 'Not able to reach any scan under the speed limit'
            return -1, 0, 0, 0, messages

        if equi_rect_distance(loc, worker_loc) > (now_date - last_action).total_seconds() * self.args.kph / 3600:

            messages['wait'] = 'Moving {}m to step {} for a {}'.format(
                int(equi_rect_distance(loc, worker_loc) * 1000), step, best['kind'])
            return -1, 0, 0, 0, messages

        prefix += ' Step %d,' % (step)
        # Check again if another worker heading there
        if item.get('done', False):
            messages['wait'] = 'Skipping step {}. Other worker already scanned.'.format(step)
            return -1, 0, 0, 0, messages

        if not self.ready:
            messages['wait'] = 'Search aborting. Overseer refreshing queue.'
            return -1, 0, 0, 0, messages

        # if a new band, set the date to wait until for the next band
        if best['kind'] == 'band' and best['end'] - best['start'] > 5 * 60:
            self.next_band_date = datetime.utcnow() + timedelta(seconds=self.band_spacing)

        # Mark scanned
        item['done'] = 'Scanned'
        status['index_of_queue_item'] = i

        messages['search'] = 'Scanning step {} for a {}'.format(best['step'], best['kind'])
        return best['step'], best['loc'], 0, 0, messages

    def task_done(self, status, parsed=False):
        if parsed:
            # Record delay between spawn time and scanning for statistics
            now_secs = date_secs(datetime.utcnow())
            item = self.queues[0][status['index_of_queue_item']]
            seconds_within_band = int((datetime.utcnow() - self.refresh_date).total_seconds()) + self.refresh_ms
            start_delay = seconds_within_band - item['start'] - (self.args.spawn_delay if item['kind'] == 'spawn' else 0)
            safety_buffer = item['end'] - seconds_within_band

            if safety_buffer < 0:
                log.warning('Too late by %d sec for a %s at step %d', -safety_buffer, item['kind'], item['step'])

            # If we had a 0/0/0 scan, then unmark as done so we can retry, and save for Statistics
            elif parsed['bad_scan']:
                self.scans_missed_list.append(cellid(item['loc']))
                item['done'] = None
                log.info('Putting back step %d in queue', item['step'])
            else:
                # Scan returned data
                self.scans_done += 1
                item['done'] = start_delay

                # Were we looking for spawn?
                if item['kind'] == 'spawn':

                    sp_id = item['sp']
                    # Did we find the spawn?
                    if sp_id in parsed['sp_id_list']:
                        self.spawns_found += 1
                    elif start_delay > 0:  # not sure why this could be negative, but sometimes it is

                        # if not, record ID and put back in queue
                        self.spawns_missed_delay[sp_id] = self.spawns_missed_delay.get(sp_id, [])
                        self.spawns_missed_delay[sp_id].append(start_delay)
                        item['done'] = 'Scanned'

                # For existing spawn points, if in any other queue items, mark 'scanned'
                for sp_id in parsed['sp_id_list']:
                    for item in self.queues[0]:
                        if (sp_id == item.get('sp', None) and item.get('done', None) is None and
                                now_secs > item['start'] and now_secs < item['end']):
                            item['done'] = 'Scanned'


# The SchedulerFactory returns an instance of the correct type of scheduler.
class SchedulerFactory():
    __schedule_classes = {
        "hexsearch": HexSearch,
        "hexsearchspawnpoint": HexSearchSpawnpoint,
        "spawnscan": SpawnScan,
        "speedscan": SpeedScan,
    }

    @staticmethod
    def get_scheduler(name, *args, **kwargs):
        scheduler_class = SchedulerFactory.__schedule_classes.get(name.lower(), None)

        if scheduler_class:
            return scheduler_class(*args, **kwargs)

        raise NotImplementedError("The requested scheduler has not been implemented")
