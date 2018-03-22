import logging

import s3fs

from skills_utils.iteration import Batch
from skills_utils.s3 import S3BackedJsonDict
from skills_ml.job_postings import JobPosting


class BratExperiment(object):
    """Manage a BRAT experiment. Handles:

    1. The creation of BRAT config for a specific sample of job postings
    2. Adding users to the installation and allocating them semi-hidden job postings
    3. The parsing of the annotation results at the end of the experiment

    Syncs data to an experiment directory on S3.
    BRAT installations are expected to sync this data down regularly.
    """
    def __init__(self, experiment_name, brat_s3_path):
        self.experiment_name = experiment_name
        self.brat_s3_path = brat_s3_path
        self.metadata = S3BackedJsonDict(
            path=self.experiment_path + '/metadata'
        )
        self.user_pw_store = S3BackedJsonDict(
            path=self.experiment_path + '/user-pws'
        )
        self.s3 = s3fs.S3FileSystem()

    @property
    def experiment_path(self):
        "The s3 path to all files relating to the experiment"
        return '/'.join([self.brat_s3_path, self.experiment_name])

    @property
    def brat_config_path(self):
        "The s3 path to BRAT config files for the experiment"
        return '/'.join([self.experiment_path, 'brat_config'])

    @property
    def data_path(self):
        "The s3 path to job postings for the experiment"
        return '/'.join([self.brat_config_path, 'data'])

    def unit_path(self, unit_name):
        "The s3 path to job postings for a particular unit"
        return '/'.join([self.data_path, '.' + unit_name])

    def user_allocations_path(self, user_name):
        "The s3 path to all allocations for a user (i.e what they should see when logging in"
        return '/'.join([self.data_path, '.' + user_name])

    def allocation_path(self, user_name, unit_name):
        "The s3 path for a particular allocation for a user"
        return '/'.join([self.user_allocations_path(user_name), unit_name])

    def start(
        self,
        sample,
        entities_with_shortcuts,
        minimum_annotations_per_posting=2,
        max_postings_per_allocation=10,
    ):
        """Starts a BRAT experiment by dividing up a job posting sample into units
            and creating BRAT config files

        Args:
            sample (skills_ml.algorithms.sampling.Sample) A sample of job postings
            entities_with_shortcuts (collection of tuples) The distinct entities to tag.
                The first entry of each tuple should be a one character string
                    that can be used as a keyboard shortcut in BRAT
                The second entry of each tuple should be the name of the entity
                    that shows up in menus
            minimum_annotations_per_posting (int, optional) How many people should annotate
                each job posting before allocating new ones. Defaults to 2
            max_postings_per_allocation (int, optional) How many job postings for each allocation.
                Should be a number that is not so high as to be daunting for users at first,
                but not so low as to make it a hassle to do several given that requesting
                new allocations is not automatic.
                Defaults to 10
        """
        logging.info('Starting experiment! Wait for a bit')
        self.metadata['sample_base_path'] = sample.base_path
        self.metadata['sample_name'] = sample.name
        self.metadata['entities_with_shortcuts'] = entities_with_shortcuts
        self.metadata['minimum_annotations_per_posting'] = minimum_annotations_per_posting
        self.metadata['max_postings_per_allocation'] = max_postings_per_allocation

        # 1. Output job posting text
        self.metadata['units'] = {}
        logging.info('Dividing sample into bundles of size %s', max_postings_per_allocation)
        for unit_num, batch_postings in enumerate(Batch(sample, max_postings_per_allocation)):
            unit_name = 'unit_{}'.format(unit_num)
            self.metadata['units'][unit_name] = []
            for posting_key, posting_string in enumerate(batch_postings):
                posting = JobPosting(posting_string)
                self.metadata['units'][unit_name].append((posting_key, posting.id))
                outfilename = '/'.join([self.unit_path(unit_name), str(posting_key)])
                logging.info('Writing to %s', outfilename)
                with self.s3.open(outfilename + '.txt', 'wb') as f:
                    f.write(posting.text.encode('utf-8'))
                with self.s3.open(outfilename + '.ann', 'wb') as f:
                    f.write(''.encode('utf-8'))
        self.metadata.save()
        logging.info('Done creating bundles. Now creating BRAT configuration')

        # 2. Output annotation.conf with lists of entities
        with self.s3.open('/'.join([self.brat_config_path, 'annotation.conf']), 'wb') as f:
            f.write('[entities]\n'.encode('utf-8'))
            for _, entity_name in entities_with_shortcuts:
                f.write(entity_name.encode('utf-8'))
                f.write('\n'.encode('utf-8'))
            f.write('[relations]\n\n# none defined'.encode('utf-8'))
            f.write('[attributes]\n\n# none defined'.encode('utf-8'))
            f.write('[events]\n\n# none defined'.encode('utf-8'))

        # 3. Output kb_shortcuts.conf with quick type selection for each entity
        with self.s3.open('/'.join([self.brat_config_path, 'kb_shortcuts.conf']), 'wb') as f:
            for shortcut, entity_name in entities_with_shortcuts:
                to_write = shortcut + ' ' + entity_name + '\n'
                f.write(to_write.encode('utf-8'))

        # 4. Output visual.conf with list of entities
        with self.s3.open('/'.join([self.brat_config_path, 'visual.conf']), 'wb') as f:
            f.write('[labels]\n'.encode('utf-8'))
            for _, entity_name in entities_with_shortcuts:
                f.write(entity_name.encode('utf-8'))
                f.write('\n'.encode('utf-8'))

        logging.info('Done creating BRAT configuration. All data is at %s', self.experiment_path)

    def add_user(self, username, password):
        """Creates a user with an allocation

        Args:
            username (string) The desired username
            password (string) The desired password
        """
        # Creates a user with an allocation.

        if username in self.user_pw_store:
            raise ValueError('User {} already created'.format(username))
        self.user_pw_store[username] = password
        self.user_pw_store.save()
        return self.add_allocation(username)

    def needs_allocation(self, unit_name):
        """Whether or not this unit needs to be allocated again.

        Args:
            unit_name (string) The name of a unit (from experiment's .metadata['units']

        Returns: (bool) Whether or not the unit should be allocated again
        """
        return sum([
            1
            for user_units in self.metadata['allocations'].values()
            if unit_name in user_units
        ]) < self.metadata['minimum_annotations_per_posting']

    def add_allocation(self, user_name):
        """Allocate a unit of job postings to the given user

        Args:
            user_name (string) A username (expected to be created already with a password)

        Returns: (string) The directory containing job postings in the allocation
        """
        # given a user name
        if user_name not in self.user_pw_store:
            raise ValueError('Username not in user-password store. Please call add_user first')

        # initialize allocations if there have been none yet
        if 'allocations' not in self.metadata:
            self.metadata['allocations'] = {}
        if user_name not in self.metadata['allocations']:
            self.metadata['allocations'][user_name] = []

        # see if there is a next unit that the user hasn't seen and really needs allocation
        unit_to_allocate = None
        try:
            unit_to_allocate = next(
                unit_name
                for unit_name in self.metadata['units'].keys()
                if unit_name not in self.metadata['allocations'][user_name]
                and self.needs_allocation(unit_name)
            )
        except StopIteration:
            pass

        # if there is none that really needs allocation
        # just pick the next one they haven't seen yet
        if not unit_to_allocate:
            try:
                unit_to_allocate = next(
                    unit_name
                    for unit_name in self.metadata['units'].keys()
                    if unit_name not in self.metadata['allocations'][user_name]
                )
            except StopIteration:
                pass

        if not unit_to_allocate:
            raise ValueError('No units left to allocate to user!')

        # create and populate a directory for the user that has the contents of the unit
        source_dir = self.unit_path(unit_to_allocate)
        dest_dir = self.allocation_path(user_name, unit_to_allocate)

        for source_key in self.s3.ls(source_dir):
            dest_key = source_key.replace(source_dir, dest_dir)
            self.s3.copy(source_key, dest_key)

        # record in metadata the fact that the user has been allocated this
        self.metadata['allocations'][user_name].append(unit_to_allocate)
        logging.info('Allocation created! Directory is %s', dest_dir)
        return dest_dir

    def inter_rater_reliability(self):
        raise NotImplementedError()
        # grab all annotations from all users
        # calculate inter-rater reliability and return along with stats

    def end(self):
        raise NotImplementedError()
        # grab all annotations from all users
        # merge annotations for all annotations of a given job posting according to some formula
        # upload skill_candidates
        # maybe upload ground truth labels above a certain threshold
        # as ground truth to another place?
