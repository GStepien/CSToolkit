# CSToolkit
## Overview
The **Concurrency Safety (CS) Toolkit** aims at unifying different levels of concurrency 
(single & multi thread and multiprocessing) by abstracting away the 
different low level datastructures and synchronization mechanisms individually 
associated with each one of those levels.

It does so by providing a unified concurrency API centered around the 
``concurrency.cs.Pr.HasCSLMixin`` class which
is initialized with and provides accessors to an ``IntEnum`` variable of type ``concurrency.cs.En.CSL``
("CSL" for "concurrency safety level"). 

(Note the distinction between ``concurrency.cs.Pr.HasCSLMixin`` - which is a ``Protocol`` - and
``concurrency.cs.Im.HasCSLMixin`` - which is an implementation of said ``Protocol``. Protocol
subclasses of ``HasCSLMixin`` should inherit from the former while implementations should
inherit from the latter. This pattern can be found throughout this library and aims at
achieving a separation of interface (e.g., to be used for type annotations) and implementation. See also 
[subsection about the intra module structure](#intra-module-structure).)

The ``concurrency.cs.En.CSL`` enum has the values specified below. 
Each of them comes with certain guarantees which implementations of ``HasCSLMixin`` must ensure:
* ``SINGLE_THREAD``: The ``HasCSLMixin`` subclass instance does not have to ensure any kind of
concurrency safety.
* ``MULTI_THREAD``: The ``HasCSLMixin`` subclass instance guarantees 
concurrency safety and data synchronization for concurrent access
from different threads from **the same** (sub)process as the one from which this instance was created.
* ``MULTI_PROCESS``: The ``HasCSLMixin`` subclass instance guarantees 
concurrency safety and data synchronization for concurrent access
from different threads, including those from **different** subprocesses. 
Most of the ``HasCSLMixin`` subclasses
provided by this library use ``multiprocessing.managers.SyncManager`` instances to handle 
data synchronization on a lower level when CSL is set to ``MULTI_PROCESS``. 
This is why most implementations (like, for example,
``concurrency.csdata.CSMutableLengthSequence`` which is basically a list-like class
which also implements the ``HasCSLMixin`` contract) also require an optional manager instance in their
constructor which must be non-``None`` if CSL is ``MULTI_PROCESSING``.

This simplifies the typically error prone implementation of concurrency safe data structures and
allows to "scale" the level of concurrency safety and synchronization based on the actual 
needs by simply providing a different ``concurrency.cs.En.CSL`` value to the constructor
of the respective ``HasCSLMixin`` subclass. 

All a programmer of such a custom ``HasCSLMixin`` subclass has to take care of is
that, given the implementation is initialized with a CSL ``c``, all the internal and external fields
that the implementation's methods might access have a CSL of **at least** ``c`` - either 
explicitly by checking whether those fields are ``HasCSLMixin`` instances with an appropriate CSL, 
or implicitly by initializing those
fields to appropriate low level Python fields (e.g., by using a ``multiprocessing.managers.SyncManager``
instance in the case of ``c == MULTI_PROCESSING`` or by using data structures from
the Python ``threading`` package in case of ``c == MULTI_THREAD``).

Particularly useful classes from the **CSToolkit** are ``concurrency.cslocks.Pr.CSLockableMixin`` and
``concurrency.csrwlocks.Pr.CSRWLockableMixin`` and their respective implementations from
the ``Im`` namespace in the same packages. Both implement ``HasCSLMixin`` and provide
mechanisms for conveniently locking access to methods of their custom subclasses via decorators. 
Those decorators are ``De.cslock`` for``CSLockableMixin`` subclasses 
and ``De.csrlock`` & ``De.cswlock`` for ``CSRWLockableMixin`` subclasses. 
Like the names suggest, the first class provides a decorator for regular, non-reentrant method 
locking and the second class provides read and non-reentrant write locking decorators
(based on a reader-writer lock implementation from ``concurrency.csrwlocks``). 
All methods decorated by any of these lock decorators must
accept at least the following parameters ``blocking: bool = True``, ``timeout: Optional[float] = None`` and
``_unsafe: bool = False`` (note that while for ``blocking`` and ``timeout``, the 
default values provided here are not mandatory but recommended, the ``_unsafe``
parameter **must** either have no default parameter at all or be ``False`` by default). While the first two parameters should be self explanatory for those familiar
with Python locks, the last one can be used to skip
the lock acquisition and should thus be used with caution (this is the reason for the ``_``-prefix as this
feature should only be used by subclasses or other methods from the same class).
A typical use-case is when one method decorated with a non-reentrant
lock calls a different one which is also decorated with the same non-reentrant lock. Without the means
to skip the lock acquisition in the second call, this would end up in a deadlock.
It is also possible to provide specific lock keys to the constructor of both ``CSLockableMixin``
and ``CSRWLockableMixin`` and to decorate different methods with different locks (for example,
by decorating one set of methods with ``@De.csrlock("custom_lock_name")`` where "custom_lock_name" is a 
key provided via the ``csrwlock_keys`` parameter of the constructor and a different 
set of methods with ``@De.csrlock`` which implicitly uses the default
lock key ``concurrency.cslocks.Co.f_DEFAULT_CSLOCK_KEY``). All locks are created
instance wise - i.e., two different instances of ``CSLockableMixin`` do not share their internal locks
(the same holds for two different instances of ``CSRWLockableMixin``).

Note that this is just a preview of the most important concepts. This library 
also provides other utilities such as:
* ``concurrency.csrun.Pr.CSRunnableMixin`` which is a ``HasCSLMixin`` 
subclass that can be executed by an instance of a ``concurrency.csrun.CSRunnableManager``
based on the former's CSL:
  * ``SINGLE_THREAD``: Run in single thread mode. This basically 
  boils down to a regular method call.
  * ``MULTI_THREAD``: Run in a separate thread.
  * ``MULTI_PROCESS``: Run in a separate thread in a newly spawned subprocess.
* Different data structures and synchronization classes - all of which implement ``HasCSLMixin``.
Examples are locks, reader-writer locks, 
condition objects, values, sequences (former two also providing a ``c_apply`` method for
applying arbitrarily complex operations in an atomic manner) and queues.
* Picklable managers from ``concurrency.managers`` which allow for the sharing of manager instances 
across processes in order to be able to create and share a new ``HasCSLMixin`` instance
after a subprocess has already started (many ``HasCSLMixin`` subclasses often require a 
manager instance in their constructor in the case of CSL being``MULTI_PROCESS``).
* Various generic programming tools for typing, inspection and testing (with a special focus
on tools for testing code meant to be executed concurrently).

## Tests
### Mypy only
In order to run a static typecheck, open a terminal, go to the project base folder (assuming you have all dependencies
from ``setup.py`` installed) and type: 

``mypy src/``

### Execute all tests
For most of the code provided here - especially the one in the ``concurrency`` package - extensive
tests have been written in **pytest**. In order to run them all (including a mypy static typecheck),
open a terminal and execute the following command from the project base folder (assuming you have all dependencies
from ``setup.py`` installed):

``pytest --color=yes --mypy --verbose -x  -s src/``

Note that many of the concurrency tests are implemented by testing for various timeouts. This results
in the testing typically taking 1-2h.

## Coding Conventions
### Intra module structure
The contents of most modules are grouped into one or multiple of the following namespaces (implemented by
nesting the contents into classes which serve as "namespaces"):
* En: Contains ``enum`` instances.
* Pr: Contains ``Protocols``.
* Ab: Contains abstract classes.
* Im: Contains non-abstract classes.
* Ca: Contains callables (typically implemented as static methods of class Ca).
* Er: Contains custom Exceptions.
* Wa: Contains custom Warnings.
* Co: Contains constants.
* De: Contains decorators.

### Naming conventions
Most names follow the typical Python naming conventions (e.g., class names as upper camel case,
variables and methods as lower snake case). In order to make efficient use of IDE code completion 
functionalities, the following additional conventions are used throughout this project:
* Everything callable (**including** callables like lambdas assigned to local variables inside methods) 
has a ``c_*``, ``_c_*`` or ``__c_*`` prefix (for public, protected or private fields, respectively).
* Every non-callable field which is **not** a local variable inside a method and is **not**
an enum field has a ``f_*``, ``_f_*`` or ``__f_*`` prefix (for public, protected or private fields, 
respectively).
* Non-callable constant fields which are **not** a local variable inside a method have a ``Final`` type
annotation and an all uppercase camel case name except for the ``f``-prefix rule from the last
bullet point which still applies.
* Enum fields have an all uppercase camel case name.


## Version
Release versions correspond to commits to the `master` branch with a commit tag `<version>-RELEASE`. Checkout this version via `git checkout <version>-RELEASE`.

The current release version is: `0.1.0`.

## License
See [`./LICENSE.md`](./LICENSE.md).