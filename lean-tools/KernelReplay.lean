/-
Kernel replay of compiled project modules (protocol task-based-lean-replay@1).

Every declaration the requested modules recorded is sent to the kernel again by
`Lean.Kernel.Environment.replay`, against one environment of their other imports,
which are trusted as compiled. This is the algorithm of the toolchain's `leanchecker`
(LeanChecker.lean, Copyright (c) 2023 Kim Morrison, Sebastian Ullrich, Apache 2.0),
loading the imported environment once for all requested modules rather than once
for each of them.

Modules replayed together can reach one name twice, which leanchecker, replaying one
module at a time, never meets: two of their imports may both declare it, an import may
itself reach a requested module, or two requested modules may both declare it. Lean then
refuses the import, or the kernel rejects the second declaration as already declared: a
fault of the grouping, not of either module. So a request of several modules reports such
a name as held, before it replays anything, for the caller to replay them in smaller
groups. One module alone is replayed exactly as leanchecker replays it.
-/
import Lean.CoreM
import Lean.Replay

open Lean

/-- All data parts of a compiled module: exported, server and private. -/
unsafe def moduleParts (module : Name) : IO (Array (ModuleData × CompactedRegion)) := do
  let mFile ← findOLean module
  unless (← mFile.pathExists) do
    throw <| IO.userError s!"object file '{mFile}' of module {module} does not exist"
  let mut fnames := #[mFile]
  let sFile := OLeanLevel.server.adjustFileName mFile
  if (← sFile.pathExists) then
    fnames := fnames.push sFile
    let pFile := OLeanLevel.private.adjustFileName mFile
    if (← pFile.pathExists) then
      fnames := fnames.push pFile
  readModuleDataParts fnames

/-- Whether replay takes `a` and `b` for one declaration: the same theorem, which it skips
when the environment already holds it, as Lean's import accepts it twice. -/
def sameTheorem : ConstantInfo → ConstantInfo → Bool
  | .thmInfo a, .thmInfo b =>
    a.name == b.name && a.type == b.type && a.levelParams == b.levelParams && a.all == b.all
  | _, _ => false

/-- The requested modules' constants, where each came from, their other imports, and the
first name two requested modules both declare, other than the same theorem. -/
unsafe def load (modules : Array Name) :
    IO (Environment × Std.HashMap Name ConstantInfo × Std.HashMap Name Name × Option Name) := do
  let requested : NameSet := modules.foldl (·.insert ·) {}
  let mut imports : Array Import := #[]
  let mut constants : Std.HashMap Name ConstantInfo := {}
  let mut origin : Std.HashMap Name Name := {}
  let mut twice : Option Name := none
  for module in modules do
    let parts ← moduleParts module
    let some (data, _) := parts[0]? | throw <| IO.userError s!"failed to read module data of {module}"
    for i in data.imports do
      unless requested.contains i.module do
        imports := imports.push i
    -- The last ("most private") part subsumes the earlier ones.
    let some (last, _) := parts.back? | throw <| IO.userError s!"failed to read module data of {module}"
    for name in last.constNames, info in last.constants do
      if let some first := constants[name]? then
        if twice.isNone && !sameTheorem first info then
          twice := some name
      constants := constants.insert name info
      origin := origin.insert name module
  let (_, state) ← importModulesCore imports |>.run
  let env ← finalizeImport state imports {} 0 false false (isModule := true)
  return (env, constants, origin, twice)

/-- The first requested declaration that replay would send to the kernel while the
environment already holds its name, other than the same theorem, which replay skips. -/
def held (env : Kernel.Environment) (constants : Std.HashMap Name ConstantInfo) : Option Name := Id.run do
  for (name, info) in constants do
    if !info.isUnsafe && !info.isPartial then
      if let some other := env.find? name then
        unless sameTheorem other info do
          return some name
  return none

/-- The name Lean's import names when two imported modules declare it. -/
def importedTwice (message : String) : Option String :=
  match message.splitOn "environment already contains '" with
  | _ :: rest :: _ => (rest.splitOn "'").head?
  | _ => none

/-- Report a name the requested modules reach twice; exit 4. -/
def reportHeld (outputFile : System.FilePath) (name : String) (module : Option Name) : IO UInt32 := do
  IO.FS.writeFile outputFile (Json.mkObj [
    ("protocol", toJson "task-based-lean-replay@1"), ("held", toJson name),
    ("module", toJson (module.map toString))]).compress
  return 4

/-- The declaration that `replay` names when the kernel rejects one. -/
def replayedDeclaration (message : String) : Option Name :=
  match message.splitOn "while replaying declaration '" with
  | _ :: rest :: _ => (rest.splitOn "'").head?.map String.toName
  | _ => none

unsafe def main (args : List String) : IO UInt32 := do
  let [sysroot, requestFile, outputFile] := args
    | IO.eprintln "Usage: KernelReplay.lean SYSROOT REQUEST OUTPUT"; return 2
  -- A failure before the replay concerns the environment, such as a compiled file
  -- that could not be read, not the replayed declarations.
  let requested ← try
      initSearchPath sysroot
      let request ← IO.ofExcept (Json.parse (← IO.FS.readFile requestFile))
      let names ← IO.ofExcept (request.getObjValAs? (Array String) "modules")
      pure (Except.ok (names.map String.toName))
    catch error =>
      pure (Except.error error.toString)
  if let .error message := requested then
    IO.eprintln s!"Environment unavailable: {message}"
    return 3
  let modules := requested.toOption.getD #[]
  let several := modules.size > 1
  let loaded ← try
      pure (Except.ok (← load modules))
    catch error =>
      pure (Except.error error.toString)
  match loaded with
  | .error message =>
    if several then
      if let some name := importedTwice message then
        return ← reportHeld outputFile name none
    IO.eprintln s!"Environment unavailable: {message}"
    return 3
  | .ok (env, constants, origin, twice) =>
    let kernel := env.toKernelEnv
    if several then
      if let some name := twice <|> held kernel constants then
        return ← reportHeld outputFile name.toString (origin.get? name)
    try
      discard <| kernel.replay constants
      IO.FS.writeFile outputFile (Json.mkObj [
        ("protocol", toJson "task-based-lean-replay@1"), ("verdict", toJson "verified"),
        ("modules", toJson modules.size), ("constants", toJson constants.size)]).compress
      return 0
    catch error =>
      let message := error.toString
      let module := (replayedDeclaration message).bind (origin.get? ·)
      let finding := Json.mkObj [
        ("protocol", toJson "task-based-lean-replay@1"), ("finding", toJson message),
        ("module", toJson (module.map toString))]
      IO.FS.writeFile outputFile finding.compress
      IO.println finding.compress
      IO.eprintln message
      return 1
