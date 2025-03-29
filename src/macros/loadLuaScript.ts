import * as path from "node:path";
import * as fs from "node:fs";

/**
 * Macro function executed at bundle time to load Lua script content.
 * @param scriptName The base name of the Lua script (e.g., "addJob").
 * @returns The content of the Lua script file as a string.
 */
export function loadLuaScriptContent(scriptName: string): string {
  // Construct the path relative to *this macro file's location*.
  // Assuming your lua scripts are in a 'lua' directory one level up
  // from the 'macros' directory (e.g., src/lua/*.lua). Adjust if needed.
  const scriptPath = path.join(
    import.meta.dirname,
    `../scripts/lua/${scriptName}.lua`,
  );
  console.log(
    `[Macro] Loading Lua script '${scriptName}' from path: ${scriptPath}`,
  );

  try {
    // Read the file synchronously to get the content
    const content = fs.readFileSync(scriptPath, "utf-8");
    // Return the content of the Lua script
    return content;
  } catch (err) {
    console.error(
      `[Macro Error] Failed to load Lua script '${scriptName}' from path: ${scriptPath}`,
      err,
    );
    // Throwing an error here will cause the bun build process to fail.
    throw new Error(`[Macro Error] Could not load Lua script: ${scriptName}`);
  }
}
