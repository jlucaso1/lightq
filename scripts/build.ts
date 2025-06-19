await Bun.build({
	entrypoints: ["./src/index.ts"],
	outdir: "./dist",
	target: "node",
	external: ["croner", "ioredis"],
});
export {};
