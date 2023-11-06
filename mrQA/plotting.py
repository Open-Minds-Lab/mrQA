from collections import defaultdict

from bokeh.embed import components
from bokeh.plotting import figure
from protocol import UnspecifiedType

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
                    secondary_value = seq[param_secondary].get_value()
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
                normalized_counts[key1][key2] = (
                    counter[key1][key2] / base_counter[key1][key2])

        if not normalized_counts:
            raise ValueError("Primary counter is empty. "
                             "No values found for normalization")
        self.uniq_secondary_values = uniq_secondary_values
        return dict(sorted(normalized_counts.items()))

    def pad_with_zeroes(self, normalized_counts, primary_param):
        """Pad the normalized counts with zeroes"""
        data = {
            primary_param: []
        }

        # initialize data
        for category in self.uniq_secondary_values:
            data[category] = []

        for key, values_by_category in normalized_counts.items():
            data[primary_param].append(key)
            for category in self.uniq_secondary_values:
                if category not in values_by_category:
                    data[category].append(0)
                else:
                    data[category].append(values_by_category[category])
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


class MultiLinePlot(MultiPlot):
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
        label = list(data.keys())[0]
        self.set_cmap(len(self.uniq_secondary_values))

        p = figure(x_range=self.x_range,
                   y_axis_label=self.y_axis_label,
                   width=self.plot_width, height=self.plot_height)
        for i, k in enumerate(self.uniq_secondary_values):
            try:
                p.line(x=label, y=k, source=data, line_width=self.line_width,
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


class ManufacturerAndDate(MultiLinePlot):
    """Plot for Manufacturer and Date"""
    def __init__(self):
        super().__init__(plot_height=600, plot_width=800)
        self.parameters = ['ContentDate', 'Manufacturer']


class PatientSexAndAge(MultiLinePlot):
    """Plot for PatientSex and PatientAge"""
    def __init__(self):
        super().__init__(plot_height=600, plot_width=800)
        self.parameters = ['PatientAge', 'PatientSex']


class ManufacturersModelAndDate(MultiLinePlot):
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


class SoftwareVersionsAndDate(MultiLinePlot):
    """Plot for Manufacturer and Date"""
    def __init__(self):
        super().__init__(plot_height=600, plot_width=800)
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
