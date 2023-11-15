import csv
from ast import literal_eval
from collections import defaultdict
from pathlib import Path

from bokeh.embed import components
from bokeh.models import FactorRange, ColumnDataSource
from bokeh.plotting import figure
from protocol import UnspecifiedType

from mrQA import logger
from mrQA.base import BasePlot
from mrQA.utils import previous_month, next_month


class MultiPlot(BasePlot):
    def __init__(self, name=None):
        super().__init__(name=name)
        self.uniq_secondary_values = None
        self.parameters = None

    def get_counter(self, dataset, parameters):
        """Computes the counter for the given dataset and parameters."""
        counter = {}
        if len(parameters) > 2:
            raise ValueError("MultiPlot can only plot two parameters")
        param_primary, param_secondary = parameters
        for seq_name in dataset.get_sequence_ids():
            for subj, sess, run, seq in dataset.traverse_horizontal(
                    seq_name):
                try:
                    primary_value = seq[param_primary].get_value()
                    if 'MEDICALSYSTEMS' in primary_value:
                        primary_value = primary_value.split('MEDICALSYSTEMS')[0]
                    secondary_value = seq[param_secondary].get_value()
                    if 'ORCHESTRASDK' in secondary_value:
                        continue
                except KeyError:
                    continue

                if (isinstance(primary_value, UnspecifiedType) or
                        isinstance(secondary_value, UnspecifiedType)):
                    continue
                if primary_value not in counter:
                    counter[primary_value] = {}
                if secondary_value not in counter[primary_value]:
                    counter[primary_value][secondary_value] = 0
                counter[primary_value][secondary_value] += 1
        return counter

    def normalize_counts(self, counter, base_counter):
        """Normalize the values in counter by values of base counter"""
        normalized_counts = defaultdict(dict)
        uniq_secondary_values = set()

        for key1 in counter:
            for key2 in counter[key1]:
                uniq_secondary_values.add(key2)
                normalized_counts[key1][key2] = 100*(
                    counter[key1][key2] / base_counter[key1][key2])

        if not normalized_counts:
            raise ValueError("Primary counter is empty. "
                             "No values found for normalization")
        self.uniq_secondary_values = sorted(list(uniq_secondary_values))
        return dict(sorted(normalized_counts.items()))

    def pad_with_zeroes(self, normalized_counts, primary_param):
        """Pad the normalized counts with zeroes"""
        data = {
            primary_param: []
        }

        # initialize data
        for category in self.uniq_secondary_values:
            data[category] = {
                'x': [],
                'y': []
            }

        for key, values_by_category in normalized_counts.items():
            data[primary_param].append(key)
            for category in values_by_category:
                # if category not in values_by_category:
                #     continue
                #     # data[category].append(0)
                # else:
                data[category]['x'].append(key)
                data[category]['y'].append(values_by_category[category])
        return data

    def compute_counts(self, non_compliant_ds, complete_ds, parameters):
        """Returns the plot components for the given dataset and parameters."""
        counter = self.get_counter(non_compliant_ds, parameters)
        base_counter = self.get_counter(complete_ds, parameters)
        normalized_counts = self.normalize_counts(counter, base_counter)
        data = self.pad_with_zeroes(normalized_counts, parameters[0])
        return data

    def plot(self, non_compliant_ds, complete_ds, parameters):
        """Creates a plot for the given data"""
        raise NotImplementedError


class BarPlot(MultiPlot):
    """Plot for creating bar plots"""
    _name = 'bar_plot'

    def __init__(self, legend_label=None, y_axis_label='% Deviations',
                 plot_height=300,
                 plot_width=800):
        super().__init__(name=self._name)
        self.legend_label = legend_label
        self.y_axis_label = y_axis_label
        self.plot_width = plot_width
        self.plot_height = plot_height

    def compute_counts(self, non_compliant_ds, complete_ds, parameters):
        """Returns the plot components for the given dataset and parameters."""
        counter = self.get_counter(non_compliant_ds, parameters)
        base_counter = self.get_counter(complete_ds, parameters)
        normalized_counts = self.normalize_counts(counter, base_counter)
        data = self.pad_with_zeroes(normalized_counts, parameters[0])
        return data

    def pad_with_zeroes(self, normalized_counts, primary_param):
        """Pad the normalized counts with zeroes"""
        factors = [(str(i), str(j)) for i in normalized_counts
                   for j in normalized_counts[i]]
        y = [normalized_counts[i][j] for i in normalized_counts
             for j in normalized_counts[i]]

        self.x_range = FactorRange(*factors)
        source = ColumnDataSource(data=dict(
            factors=factors,
            y=y
        ))

        return source

    def normalize_counts(self, counter, base_counter):
        """Normalize the values in counter by values of base counter"""
        normalized_counts = defaultdict(dict)
        uniq_secondary_values = set()

        for key1 in counter:
            for key2 in counter[key1]:
                uniq_secondary_values.add(key2)
                normalized_counts[key1][key2] = 100*(
                    counter[key1][key2] / base_counter[key1][key2])

        if not normalized_counts:
            raise ValueError("Primary counter is empty. "
                             "No values found for normalization")
        self.uniq_secondary_values = sorted(list(uniq_secondary_values))
        return dict(sorted(normalized_counts.items()))

    def get_plot_components(self, data):
        # label = list(data.keys())[0]
        self.set_cmap(3)

        p = figure(x_range=self.x_range,
                   y_axis_label=self.y_axis_label,
                   width=self.plot_width, height=self.plot_height)
        # for i, k in enumerate(self.uniq_secondary_values):
        try:
            p.vbar(x='factors', top='y', source=data, width=self.width,
                   fill_color=self.colors[2], fill_alpha=0.75,
                   line_color=self.colors[0])
        except IndexError:
            print("Unable to plot, Color index  out of range")

        p.xaxis.major_label_orientation = "vertical"
        p.xgrid.grid_line_color = None
        p.ygrid.grid_line_alpha = 0.5
        p.x_range.range_padding = 0.1

        # p.legend.click_policy = "hide"
        # p.add_layout(p.legend[0], 'below')
        return components(p)

    def plot(self, non_compliant_ds, complete_ds, parameters=None):
        """Creates a plot for the given data"""
        if not parameters:
            parameters = self.parameters
        data = self.compute_counts(non_compliant_ds, complete_ds,
                                   parameters)
        self.width = 0.8
        # self.x_range = data[parameters[0]]
        # x = data[parameters[0]]
        # for i in parameters:
        #     if 'date' in i.lower():
        #         self.x_range = (previous_month(min(x)), next_month(max(x)))
        #     if 'age' in i.lower():
        #         self.x_range = [min(x) - 0.1, max(x) + 0.1]
        #         # self.width = timedelta(days=1)

        self.div, self.script = self.get_plot_components(data)


class MultiLinePlot(MultiPlot):
    """Plot for creating multi line plots"""
    _name = 'multi_line'

    def __init__(self, legend_label=None, y_axis_label=None,  line_width=2,
                 line_dash='solid', line_alpha=0.75, plot_height=300,
                 plot_width=800):
        super().__init__(name=self._name)
        self.legend_label = legend_label
        self.line_width = line_width
        self.line_dash = line_dash
        self.line_alpha = line_alpha
        self.y_axis_label = y_axis_label
        self.plot_width = plot_width
        self.plot_height = plot_height

    def get_plot_components(self, data):
        self.set_cmap(len(self.uniq_secondary_values))

        p = figure(x_range=self.x_range,
                   y_axis_label=self.y_axis_label,
                   width=self.plot_width, height=self.plot_height)
        for i, k in enumerate(self.uniq_secondary_values):
            try:
                p.line(x=data[k]['x'], y=data[k]['y'],
                       line_width=self.line_width,
                       line_alpha=1, color=self.colors[i], legend_label=k)
            except IndexError:
                print(f"Unable to plot {k}, Color index {i} out of range")

        p.xaxis.major_label_orientation = "vertical"
        p.xgrid.grid_line_color = None
        p.ygrid.grid_line_alpha = 0.5
        p.legend.click_policy = "hide"
        p.add_layout(p.legend[0], 'below')
        return components(p)

    def plot(self, non_compliant_ds, complete_ds, parameters=None):
        """Creates a plot for the given data"""
        if not parameters:
            parameters = self.parameters
        data = self.compute_counts(non_compliant_ds, complete_ds,
                                   parameters)
        self.width = 0.9
        self.x_range = data[parameters[0]]
        x = data[parameters[0]]
        for i in parameters:
            if 'date' in i.lower():
                self.x_range = (previous_month(min(x)), next_month(max(x)))
            if 'age' in i.lower():
                self.x_range = [min(x) - 0.1, max(x) + 0.1]
                # self.width = timedelta(days=1)

        self.div, self.script = self.get_plot_components(data)


class MultiScatterPlot(MultiPlot):
    _name = 'multi_scatter'

    def __init__(self, legend_label=None, y_axis_label='% Deviations',  size=5,
                 alpha=0.75, plot_height=300,
                 plot_width=800):
        super().__init__(name=self._name)
        self.legend_label = legend_label
        self.size = size
        self.alpha = alpha
        self.y_axis_label = y_axis_label
        self.plot_width = plot_width
        self.plot_height = plot_height

    def get_plot_components(self, data):
        label = list(data.keys())[0]
        self.set_cmap(len(self.uniq_secondary_values))

        p = figure(x_range=self.x_range,
                   y_axis_label=self.y_axis_label, x_axis_label=label,
                   width=self.plot_width, height=self.plot_height)
        for i, k in enumerate(self.uniq_secondary_values):
            try:
                p.circle(x=data[k]['x'], y=data[k]['y'], size=self.size,
                         alpha=self.alpha,
                         color=self.colors[i], legend_label=k)
            except IndexError:
                print(f"Unable to plot {k}, Color index {i} out of range")

        p.xaxis.major_label_orientation = "vertical"
        p.xgrid.grid_line_color = None
        p.ygrid.grid_line_alpha = 0.5
        p.legend.click_policy = "hide"
        p.add_layout(p.legend[0], 'below')
        return components(p)

    def plot(self, non_compliant_ds, complete_ds, parameters=None):
        """Creates a plot for the given data"""
        if not parameters:
            parameters = self.parameters
        data = self.compute_counts(non_compliant_ds, complete_ds,
                                   parameters)
        self.width = 0.9
        self.x_range = data[parameters[0]]
        x = data[parameters[0]]
        for i in parameters:
            if 'date' in i.lower():
                self.x_range = (previous_month(min(x)), next_month(max(x)))
            if 'age' in i.lower():
                self.x_range = [min(x) - 0.1, max(x) + 0.1]
                # self.width = timedelta(days=1)

        self.div, self.script = self.get_plot_components(data)


class ManufacturerAndDate(MultiScatterPlot):
    """Plot for Manufacturer and Date"""
    def __init__(self):
        super().__init__(plot_height=600, plot_width=800)
        self.parameters = ['ContentDate', 'Manufacturer']


class PatientSexAndAge(BarPlot):
    """Plot for PatientSex and PatientAge"""
    def __init__(self):
        super().__init__(plot_height=600, plot_width=800)
        self.parameters = ['PatientAge', 'PatientSex']


class ManufacturersModelAndDate(MultiScatterPlot):
    """Plot for Manufacturer and Date"""
    def __init__(self):
        super().__init__(plot_height=600, plot_width=800)
        self.parameters = ['ContentDate', 'ManufacturersModelName']

    def get_counter(self, dataset, parameters):
        """Computes the counter for the given dataset and parameters."""
        counter = {}
        if len(parameters) > 2:
            raise ValueError("MultiPlot can only plot two parameters")
        param_primary, param_secondary = parameters
        for seq_name in dataset.get_sequence_ids():
            for subj, sess, run, seq in dataset.traverse_horizontal(
                    seq_name):
                try:
                    primary_value = seq[param_primary].get_value()
                    secondary_value = seq[param_secondary].get_value()
                    manufacturer = seq['Manufacturer'].get_value()
                    secondary_value = f'{manufacturer} {secondary_value}'
                except KeyError:
                    continue

                if (isinstance(primary_value, UnspecifiedType) or
                        isinstance(secondary_value, UnspecifiedType)):
                    continue
                if primary_value not in counter:
                    counter[primary_value] = {}
                if secondary_value not in counter[primary_value]:
                    counter[primary_value][secondary_value] = 0
                counter[primary_value][secondary_value] += 1
        return counter


class SoftwareVersionsAndDate(MultiScatterPlot):
    """Plot for Manufacturer and Date"""
    def __init__(self):
        super().__init__(plot_height=800, plot_width=800)
        self.parameters = ['ContentDate', 'SoftwareVersions']

    def get_counter(self, dataset, parameters):
        """Computes the counter for the given dataset and parameters."""
        counter = {}
        if len(parameters) > 2:
            raise ValueError("MultiPlot can only plot two parameters")
        param_primary, param_secondary = parameters
        for seq_name in dataset.get_sequence_ids():
            for subj, sess, run, seq in dataset.traverse_horizontal(
                    seq_name):
                try:
                    primary_value = seq[param_primary].get_value()
                    secondary_value = seq[param_secondary].get_value()
                    manufacturer = seq['Manufacturer'].get_value()
                    secondary_value = f'{manufacturer} {secondary_value}'
                except KeyError:
                    continue

                if (isinstance(primary_value, UnspecifiedType) or
                        isinstance(secondary_value, UnspecifiedType)):
                    continue
                if primary_value not in counter:
                    counter[primary_value] = {}
                if secondary_value not in counter[primary_value]:
                    counter[primary_value][secondary_value] = 0
                counter[primary_value][secondary_value] += 1
        return counter


class Site(BasePlot):
    """Plot for Manufacturer and Date"""
    def __init__(self):
        super().__init__()
        logger.warning("This plot is only for ABCD dataset")
        self.parameters = ['ContentDate', 'InstitutionName']
        self.csv_path = Path('/media/sinhah/extremessd/ABCD/1210908/original_files/abcd_lt01.txt') # noqa
        self.subject_site_map = self.get_subject_site_map()
        self.y_axis_label = '% Deviations'
        self.plot_width = 600
        self.plot_height = 300

    def get_subject_site_map(self):
        """Returns a dictionary mapping subject to site"""
        subject_site_map = {}
        with open(self.csv_path, 'r') as fh:
            reader = csv.DictReader(fh, delimiter='\t')
            for i, row in enumerate(reader):
                if i == 0:
                    continue
                subject_id = row['subjectkey']
                follow_up = row['eventname']
                site = row['site_id_l']
                if 'baseline' in follow_up.lower():
                    subject_site_map[subject_id] = site
        return subject_site_map

    def get_subject_site(self, subject_id):
        """Returns the site for the given subject"""
        return self.subject_site_map[subject_id]

    def get_counter(self, dataset, parameters):
        """Computes the counter for the given dataset and parameters."""
        counter = {}
        if len(parameters) > 2:
            raise ValueError("MultiPlot can only plot two parameters")
        # param_primary, param_secondary = parameters
        for seq_name in dataset.get_sequence_ids():
            for subj, sess, run, seq in dataset.traverse_horizontal(
                    seq_name):
                try:
                    subject_value = seq.subject_id
                    value = self.get_subject_site(subject_value)
                except KeyError:
                    continue

                if isinstance(value, UnspecifiedType):
                    continue
                if value not in counter:
                    counter[value] = 0
                counter[value] += 1
        return counter

    def compute_counts(self, non_compliant_ds, complete_ds, parameters):
        """Returns the plot components for the given dataset and parameters."""
        counter = self.get_counter(non_compliant_ds, parameters)
        base_counter = self.get_counter(complete_ds, parameters)
        normalized_counts = self.normalize_counts(counter, base_counter)
        data = self.pad_with_zeroes(normalized_counts, parameters[0])
        return data

    def pad_with_zeroes(self, normalized_counts, primary_param):
        """Pad the normalized counts with zeroes"""
        # factors = [(str(i), str(j)) for i in normalized_counts for j in normalized_counts[i]] # noqa
        # y = [normalized_counts[i][j] for i in normalized_counts for j in normalized_counts[i]] # noqa
        factors = [str(i) for i in normalized_counts]
        y = [normalized_counts[i] for i in normalized_counts]

        self.x_range = FactorRange(*factors)
        source = ColumnDataSource(data=dict(
            factors=factors,
            y=y
        ))

        return source

    def normalize_counts(self, counter, base_counter):
        """Normalize the values in counter by values of base counter"""
        normalized_counts = defaultdict(dict)
        uniq_secondary_values = set()

        for key1 in counter:
            normalized_counts[key1] = 100*(
                counter[key1] / base_counter[key1])

        if not normalized_counts:
            raise ValueError("Primary counter is empty. "
                             "No values found for normalization")
        self.uniq_secondary_values = sorted(list(uniq_secondary_values))
        return dict(sorted(normalized_counts.items()))

    def get_plot_components(self, data):
        # label = list(data.keys())[0]
        self.set_cmap(3)

        p = figure(x_range=self.x_range,
                   y_axis_label=self.y_axis_label,
                   width=self.plot_width, height=self.plot_height)
        # for i, k in enumerate(self.uniq_secondary_values):
        try:
            p.vbar(x='factors', top='y', source=data, width=self.width,
                   fill_color=self.colors[2], fill_alpha=0.75,
                   line_color=self.colors[0])
        except IndexError:
            print("Unable to plot, Color index  out of range")

        p.xaxis.major_label_orientation = "vertical"
        p.xgrid.grid_line_color = None
        p.ygrid.grid_line_alpha = 0.5
        p.x_range.range_padding = 0.1

        # p.legend.click_policy = "hide"
        # p.add_layout(p.legend[0], 'below')
        return components(p)

    def plot(self, non_compliant_ds, complete_ds, parameters=None):
        """Creates a plot for the given data"""
        if not parameters:
            parameters = self.parameters
        data = self.compute_counts(non_compliant_ds, complete_ds,
                                   parameters)
        self.width = 0.8
        # self.x_range = data[parameters[0]]
        # x = data[parameters[0]]
        # for i in parameters:
        #     if 'date' in i.lower():
        #         self.x_range = (previous_month(min(x)), next_month(max(x)))
        #     if 'age' in i.lower():
        #         self.x_range = [min(x) - 0.1, max(x) + 0.1]
        #         # self.width = timedelta(days=1)

        self.div, self.script = self.get_plot_components(data)


class ManufacturerAndModel(BarPlot):
    """Plot for Manufacturer and Model"""
    def __init__(self):
        super().__init__(plot_height=300, plot_width=400)
        self.parameters = ['Manufacturer', 'ManufacturersModelName']


class ManufacturerAndVersion(BarPlot):
    """Plot for Manufacturer and SoftwareVersion"""
    def __init__(self):
        super().__init__(plot_height=300, plot_width=400)
        self.parameters = ['Manufacturer', 'SoftwareVersions']

    def get_counter(self, dataset, parameters):
        """Computes the counter for the given dataset and parameters."""
        counter = {}
        if len(parameters) > 2:
            raise ValueError("MultiPlot can only plot two parameters")
        param_primary, param_secondary = parameters
        for seq_name in dataset.get_sequence_ids():
            for subj, sess, run, seq in dataset.traverse_horizontal(
                    seq_name):
                try:
                    primary_value = seq[param_primary].get_value()
                    if 'MEDICALSYSTEMS' in primary_value:
                        primary_value = primary_value.split('MEDICALSYSTEMS')[0]
                    secondary_value = seq[param_secondary].get_value()
                    if primary_value != 'SIEMENS':
                        secondary_value = literal_eval(secondary_value)[0]
                except KeyError:
                    continue

                if (isinstance(primary_value, UnspecifiedType) or
                        isinstance(secondary_value, UnspecifiedType)):
                    continue
                if primary_value not in counter:
                    counter[primary_value] = {}
                if secondary_value not in counter[primary_value]:
                    counter[primary_value][secondary_value] = 0
                counter[primary_value][secondary_value] += 1
        return counter
