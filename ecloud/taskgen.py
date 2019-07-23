import json, frontmatter, os
from functools import reduce
from hashlib import md5
from jinja2 import Environment, FileSystemLoader

class TaskSet:
    """A TaskSet object represents an atomic unit of computation
    to be performed on a worker. It is completely agnostic to when and
    where it is performed, as long as their performed together on the same 
    machine and as long as the dependencies are satisfied.

    The class generates the cleanup commands, packages the results for uploading
    and unpackages downloaded results.
    """

    def __init__(self, task_id, dependencies, datastore_dir='', 
            overwrite_result=True, init=False, results=[], fake=False, jinja2_env=None):
        """Results is the list of files produced by the task that will be packaged 
        into a single file named result_name.
        result_name is the name of the file that will be send back
        to the datastore WITHOUT the .gz or .tar.gz extension.
        """
        self.jinja2_env = jinja2_env
        self.task_id = task_id
        self.dependencies = dependencies 
        self.results = []
        self.requirements = []
        self.commands = []
        self.temporary = []
        self._result_name = None
        self.fake = fake
        self.init = init
        self.datastore_dir = datastore_dir
        self.overwrite_result = overwrite_result

    @property
    def hash_identifier(self):
        """Return this task's unique identifier.
        
        The uid constructed by taking the MD5 sum of the task's commands
        and the unique identifiers of its dependencies. It changes only 
        if the task's commands change or its dependencies change.
        """
        id_string = ''.join(t.hash_identifier for t in self.dependencies) + ''.join(map(str, self.requirements)) + ''.join(self.commands)
        return md5(id_string.encode()).hexdigest()

    @property
    def result_name(self):
        if self._result_name is None:
            return self.hash_identifier[:16]
        return self._result_name

    @result_name.setter
    def result_name(self, result_name):
        self._result_name = result_name

    def add_result(self, result):
        if result in self.results:
            print('WARNING: duplicate result (%s) on TaskSet (%s).' % (r, self.task_id))
        else:
            self.results.append(result)

    @property
    def filename(self):
        """Return the filename of the packaged results."""
        if len(self.results) > 1:
            return '%s.tar.gz' % self.result_name
        else:
            return '%s.gz' % self.result_name

    @property
    def clean_up_results(self):
        """Return commands that remove results and temporary files produced."""
        commands = []
        if len(self.results) > 1:
            commands += ['rm "%s"' % r for r in self.results]
        # Temporary files may have been moved or deleted; don't fail when they're not found.
        return ['rm "%s" || true' % f for f in self.temporary] + commands
    
    @property
    def package_results(self):
        """Return the set of commands that will archive and compress the
        results (this also immediately cleans up the archives to save space).
        """
        cmd = []
        if len(self.results) > 1:
            quote = lambda s: '"%s"' % s 
            cmd += ['tar -czf "%s.tar.gz" %s' % (self.result_name, ' '.join(map(quote, self.results)))]

        elif len(self.results) == 1:
            cmd += ['gzip -f "%s"' % self.results[0]]
            if self.results[0] != self.result_name:
                cmd += ['mv "%s.gz" "%s.gz"' % (self.results[0], self.result_name)]
        return cmd

    @property
    def unpack_results(self):
        """Return the set of commands that unpack the results of the requirements."""
        if len(self.results) > 1:
            return ['tar -xzf %s && rm %s' % (self.filename, self.filename)]
        elif len(self.results) == 1:
            commands = ['gunzip %s' % self.filename] 
            if self.result_name != self.results[0]:
                commands += ['mv "%s" "%s"' % (self.result_name, self.results[0])]
            return commands
        return []

    def add_task(self, template, context={}, fake=False):
        """Add a task based on a given template."""
        templ = self.jinja2_env.get_template(template).render(**context)
        task_spec = frontmatter.loads(templ)
        results = task_spec.get('results', []) 
        temporary = task_spec.get('temporary', [])
        task_requirements = [(r['datastore'], r['local']) for r in task_spec.get('requirements', [])]
        commands = task_spec.content.splitlines()
        return self._add_task(commands, requirements=task_requirements, temporary=temporary, results=results, fake=fake)

    def _add_task(self, commands, requirements=[], results=[], temporary=[], fake=False):
        self.commands += commands
        for r in results:
            self.add_result(r)
        self.temporary += [t for t in temporary if not t in self.temporary]
        self.requirements += [r for r in requirements if not r in self.requirements]
        return results

    def get_worker_commands(self, leave_no_trace=True):
        commands = []
        for d in self.dependencies:
            commands += d.unpack_results
        commands += self.commands
        commands += self.package_results
        for d in self.dependencies:
            commands += d.clean_up_results
        commands += self.clean_up_results

        return commands 

    @property
    def dict(self):
        """Return a dictionary representation of the object."""
        return {
            'init':self.init,
            'id':self.task_id,
            'commands':self.get_worker_commands(),
            'dependencies':[d.task_id for d in self.dependencies if not d.fake],
            'requirements':self.requirements + [d.filename for d in self.dependencies],
            'results':[[self.filename, os.path.join(self.datastore_dir, self.filename)]] if len(self.results) > 0 else [],
            'overwrite_result':self.overwrite_result,
        }


class TaskManager: 
    """The task manager does little more than counting tasks
    such that they can all be assigned a unique ID (the count).
    """

    task_id = 0

    def __init__(self, overwrite_result=True, datastore_dir='', template_dir=None, task_set_cls=TaskSet):
        self.task_sets = []
        self.jinja2_env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=False,
        )
        self.task_set_cls = task_set_cls
        self.datastore_dir = datastore_dir
        self.overwrite_result = overwrite_result

    def add_task_set(self, *args, **kwargs):
        """Create a task set and return it."""
        task_set = self.task_set_cls(
            str(self.task_id), *args,
            overwrite_result=self.overwrite_result,
            datastore_dir=self.datastore_dir,
            jinja2_env=self.jinja2_env, **kwargs
        )
        self.task_sets.append(task_set)
        self.task_id += 1

        return task_set

    def write_tasks(self, path):
        task_dicts = []
        datastore = []
        for task_set in self.task_sets:
            if task_set.filename in datastore:
                print('WARNING: task %s overwrites a result (%s) in the datastore.' % (task_set.task_id, task_set.filename))
            else:
                datastore.append(task_set.filename)
            if not task_set.fake:
                task_dicts.append(task_set.dict)
        print('Writing %d tasks to %s.' % (len(task_dicts), path))
        with open(path, 'w') as f:
            f.write(json.dumps(task_dicts, sort_keys=True, indent=2, separators=(',', ': ')))
