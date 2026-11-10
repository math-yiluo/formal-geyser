# Kernel replay

This helper sends every declaration recorded by the requested compiled modules to the
kernel again, using Lean's own replay (the algorithm of the toolchain's `leanchecker`).
Their other imports are loaded once and trusted as compiled. Compilation alone trusts
what elaboration recorded, so `debug.skipKernelTC` or an unchecked addition could record
a theorem the kernel never checked; replay rejects it. A kernel rejection is reported as
a finding naming the module; a failure to load the environment is not a finding.

Several modules replayed together may reach one name twice, which `leanchecker`, replaying
one module at a time, never meets: two of their imports may both declare it, an import may
reach one of the requested modules itself, or two requested modules may both declare it.
Lean would refuse the import, or the kernel would reject the second declaration as already
declared, though neither module is at fault. Before replaying anything, a request of several
modules therefore looks for such a name (the same theorem twice is one declaration to Lean's
import and to replay, so it is not one) and reports it as held, with exit status 4, so that
its caller replays the modules in smaller groups. One module alone is replayed exactly as
`leanchecker` replays it.
