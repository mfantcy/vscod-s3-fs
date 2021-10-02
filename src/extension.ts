import { S3Client } from '@aws-sdk/client-s3';
import { ExtensionContext, workspace } from 'vscode';
import { Config } from './config';
import { S3ClientWrapper } from './s3-client-wrapper';
import { provideCredentials } from './s3-credentials';
import { S3FileSystemProvider } from './s3-file-system-provider';

export function activate(context: ExtensionContext) {
	console.log('Congratulations "vscode-s3-fs" is now active!');
	const config = new Config(workspace.getConfiguration('vscodeS3Fs'));
	const s3Client = new S3Client({
		region: config.awsRegion || 'us-east-1',
		credentials: provideCredentials(config)
	});
	workspace.registerFileSystemProvider('s3', new S3FileSystemProvider(new S3ClientWrapper(s3Client, 10000)), { isCaseSensitive: true });
}

export function deactivate() { }
