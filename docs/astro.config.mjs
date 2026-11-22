import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';

export default defineConfig({
	site: 'https://katamaran.rs',
	trailingSlash: 'always',
	integrations: [
		starlight({
			title: 'katamaran',
			description: 'Fast Docker containers and Linux machines on Apple silicon Macs.',
			logo: { src: './src/assets/icon.png', alt: '' },
			favicon: '/favicon.png',
			head: [
				{
					tag: 'meta',
					attrs: { property: 'og:image', content: 'https://katamaran.rs/og.png' },
				},
				{
					tag: 'meta',
					attrs: { property: 'og:image:width', content: '1200' },
				},
				{
					tag: 'meta',
					attrs: { property: 'og:image:height', content: '630' },
				},
				{
					tag: 'meta',
					attrs: { name: 'twitter:card', content: 'summary_large_image' },
				},
				{
					tag: 'meta',
					attrs: { name: 'twitter:image', content: 'https://katamaran.rs/og.png' },
				},
			],
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/nklmilojevic/katamaran' },
			],
			editLink: {
				baseUrl: 'https://github.com/nklmilojevic/katamaran/edit/main/docs/',
			},
			customCss: [
				'allo-font/allo.css',
				'@fontsource-variable/jetbrains-mono',
				'./src/styles/custom.css',
			],
			expressiveCode: {
				emitExternalStylesheet: false,
				themes: ['github-light', 'tokyo-night'],
				styleOverrides: {
					borderRadius: '0.5rem',
					borderColor: ({ theme }) => (theme.type === 'dark' ? '#252c40' : '#dde4f0'),
					codeBackground: ({ theme }) => (theme.type === 'dark' ? '#1a2030' : '#f2f5fb'),
					codeFontFamily: "'JetBrains Mono Variable', ui-monospace, SFMono-Regular, monospace",
					codeFontSize: '0.85rem',
					uiFontFamily: "'Allo', ui-sans-serif, system-ui, sans-serif",
					frames: {
						editorBackground: ({ theme }) => (theme.type === 'dark' ? '#1a2030' : '#f2f5fb'),
						terminalBackground: ({ theme }) => (theme.type === 'dark' ? '#1a2030' : '#f2f5fb'),
						editorTabBarBackground: ({ theme }) => (theme.type === 'dark' ? '#1a2030' : '#f2f5fb'),
						editorActiveTabBackground: ({ theme }) => (theme.type === 'dark' ? '#1a2030' : '#f2f5fb'),
						editorActiveTabIndicatorTopColor: 'transparent',
						editorTabBarBorderBottomColor: ({ theme }) => (theme.type === 'dark' ? '#252c40' : '#dde4f0'),
						terminalTitlebarBackground: ({ theme }) => (theme.type === 'dark' ? '#1a2030' : '#f2f5fb'),
						terminalTitlebarBorderBottomColor: ({ theme }) => (theme.type === 'dark' ? '#252c40' : '#dde4f0'),
						frameBoxShadowCssValue: 'none',
					},
				},
			},
			sidebar: [
				{
					label: 'Get started',
					items: [
						{ label: 'Overview', slug: 'index' },
						'install',
						'quickstart',
					],
				},
				{
					label: 'Guides',
					items: [
						'docker',
						'file-sharing',
						'networking',
						'machines',
						'kubernetes',
						'amd64',
						'app',
						'build-from-source',
					],
				},
				{
					label: 'Reference',
					items: [
						'cli',
						'configuration',
						'benchmarks',
					],
				},
				{
					label: 'Explanation and help',
					items: [
						'architecture',
						'troubleshooting',
					],
				},
			],
			plugins: [
				starlightLinksValidator(),
			],
		}),
	],
});
