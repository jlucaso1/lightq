import * as fs from "node:fs";
import * as path from "node:path";
import luamin from "luamin";

/**
 * @internal
 * Macro function executed at bundle time to load Lua script content.
 * @param scriptName The base name of the Lua script (e.g., "addJob").
 * @returns The content of the Lua script file as a string.
 */
export function loadLuaScriptContent(scriptName: string): string {
	const scriptPath = path.join(
		import.meta.dirname,
		`../scripts/lua/${scriptName}.lua`,
	);

	try {
		// Read the file synchronously to get the content
		const content = fs.readFileSync(scriptPath, "utf-8");
		// Return the content of the Lua script
		return luamin.minify(content);
	} catch (err) {
		console.error(
			`[Macro Error] Failed to load Lua script '${scriptName}' from path: ${scriptPath}`,
			err,
		);
		// Throwing an error here will cause the bun build process to fail.
		throw new Error(`[Macro Error] Could not load Lua script: ${scriptName}`);
	}
}
