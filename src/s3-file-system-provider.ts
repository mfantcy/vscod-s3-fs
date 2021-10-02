import { Disposable, Event, EventEmitter, FileChangeEvent, FileStat, FileSystemError, FileSystemProvider, FileType, Uri } from "vscode";
import { S3ClientWrapper } from "./s3-client-wrapper";

export class S3FileSystemProvider implements FileSystemProvider {
    private emitter = new EventEmitter<FileChangeEvent[]>();
    readonly onDidChangeFile: Event<FileChangeEvent[]> = this.emitter.event;

    constructor(private s3ClientWrapper: S3ClientWrapper) {
    }

    watch(uri: Uri, options: { recursive: boolean; excludes: string[]; }): Disposable {
        return new Disposable(() => { });
    }

    async stat(uri: Uri): Promise<FileStat> {
        console.debug('stat ' + uri);
        if (uri.path === '/') {
            await this.s3ClientWrapper.listObjects(uri, { limit: 1 }).catch(getCommErrorHandler(uri));
            return {
                type: FileType.Directory,
                ctime: 0,
                mtime: 0,
                size: 0,
            };
        }
        const res = await this.s3ClientWrapper.listObjects(uri, { findMatch: true }).catch(getCommErrorHandler(uri));
        if (res.Contents && res.Contents.length > 0) {
            const c = res.Contents[0];
            const mtime = (c.LastModified?.getTime() || 0) / 1000;
            const size = c.Size || 0;
            const stat = {
                ctime: mtime,
                mtime: mtime,
                size: size,
            };
            return uri.path.endsWith('/') || `/${c.Key}`.startsWith(uri.path + '/')
                ? { type: FileType.Directory, ...stat }
                : { type: FileType.File, ...stat };
        }
        if (res.CommonPrefixes && res.CommonPrefixes.length > 0) {
            return {
                type: FileType.Directory,
                ctime: 0,
                mtime: 0,
                size: 0,
            };
        }
        throw FileSystemError.FileNotFound(uri);
    }

    async readDirectory(uri: Uri): Promise<[string, FileType][]> {
        const res = await this.list(uri);
        return res;
    }

    async list(uri: Uri, limit?: number): Promise<[string, FileType][]> {
        if (!uri.path.endsWith('/')) {
            uri = Uri.from({ ...uri.toJSON(), path: `${uri.path}/`, });
        }
        const output = await this.s3ClientWrapper.listObjects(uri, { limit }).catch(getCommErrorHandler(uri));
        const contents = output.Contents || [];
        const commonPrefix = output.CommonPrefixes || [];
        if (contents.length === 0 && commonPrefix.length === 0) {
            return [];
        }
        return (new Array<[string, FileType]>()).concat(
            commonPrefix
                .filter(c => !!c.Prefix && c.Prefix.substring(uri.path.length - 1).replace(/\/$/, ''))
                .map(c => {
                    const fileName = c.Prefix!.substring(uri.path.length - 1).replace(/\/$/, '');
                    return [fileName, FileType.Directory];
                }),
            contents
                .filter(c => !!c.Key && c.Key.substring(uri.path.length - 1).replace(/\/$/, ''))
                .map(c => {
                    const fileName = c.Key!.substring(uri.path.length - 1).replace(/\/$/, '');
                    const fileType = c.Key!.endsWith('/') ? FileType.Directory : FileType.File;
                    return [fileName, fileType];
                })
        );
    }

    async createDirectory(uri: Uri): Promise<void> {
        console.debug('createDirectory ' + uri);
        const stat = await skipNotFoundError(this.stat(uri));
        if (stat) {
            throw FileSystemError.FileExists(uri);
        }
        await this.s3ClientWrapper.createFolder(uri).catch(getCommErrorHandler(uri));
    }

    async readFile(uri: Uri): Promise<Uint8Array> {
        console.debug('readFile ' + uri);
        return this.s3ClientWrapper.getObject(uri).catch(getCommErrorHandler(uri));
    }

    async writeFile(uri: Uri, content: Uint8Array, options: { create: boolean; overwrite: boolean; }): Promise<void> {
        console.debug('writeFile ' + uri);
        let fileStat: FileStat | undefined = undefined;
        try {
            fileStat = await this.stat(uri);
        } catch (e) {
            if (!(e instanceof FileSystemError) || e.code !== 'FileNotFound') {
                throw e;
            }
        }

        if (fileStat && fileStat.type === FileType.Directory) {
            throw FileSystemError.FileIsADirectory(uri);
        }

        if (!fileStat && !options.create) {
            throw FileSystemError.FileNotFound(uri);
        }

        if (fileStat && options.create && !options.overwrite) {
            throw FileSystemError.FileExists(uri);
        }

        await this.s3ClientWrapper.putObject(uri, content);
    }

    async delete(uri: Uri, options: { recursive: boolean; }): Promise<void> {
        console.debug('delete ' + uri);
        const stat = await this.stat(uri);
        if (stat.type === FileType.Directory) {
            const res = await this.list(uri, 2);
            if (res.length > 0 && !options.recursive) {
                throw FileSystemError.NoPermissions(`${uri.toString()} is not emtpy`);
            }
            await this.s3ClientWrapper.deleteFolder(uri).catch(getCommErrorHandler(uri));
        } else {
            await this.s3ClientWrapper.deleteObject(uri).catch(getCommErrorHandler(uri));
        }
    }

    async rename(oldUri: Uri, newUri: Uri, options: { overwrite: boolean; }): Promise<void> {
        const [oldUriStat, newUriStat] = await Promise.all([
            this.stat(oldUri),
            skipNotFoundError(this.stat(newUri))
        ]);
        if (!options.overwrite && newUriStat) {
            throw FileSystemError.FileExists(newUri);
        }
        if (oldUriStat.type === FileType.Directory) {
            return this.s3ClientWrapper.copyFolder(oldUri, newUri, true).catch(getCommErrorHandler());
        }
        await this.s3ClientWrapper.copyObject(oldUri, newUri, true).catch(getCommErrorHandler());
    }

    async copy(source: Uri, destination: Uri, options: { overwrite: boolean }): Promise<void> {
        console.debug('copy ' + source + ' ' + destination);
        const [sourceStat, destinationStat] = await Promise.all([
            this.stat(source),
            skipNotFoundError(this.stat(destination))
        ]);
        if (destinationStat && !options.overwrite) {
            throw FileSystemError.FileExists(destination);
        }
        if (sourceStat.type === FileType.Directory) {
            return this.s3ClientWrapper.copyFolder(source, destination);
        }
        await this.s3ClientWrapper.copyObject(source, destination);
    }
}

const skipNotFoundError = async <T>(p: Promise<T>): Promise<T | undefined> => {
    return p.catch((e: any): Promise<undefined> => {
        if (e instanceof FileSystemError && e.code === 'FileNotFound') {
            return Promise.resolve(undefined);
        }
        return Promise.reject(e);
    });
};

const getCommErrorHandler = (uri?: Uri): ((err: any) => Promise<never>) => {
    const listOfForbidden = [
        'AccessDenied', 'AccountProblem', 'AllAccessDisabled', 'CrossLocationLoggingProhibited',
        'InvalidAccessKeyId', 'InvalidObjectState', 'InvalidPayer', 'InvalidSecurity',
        'NotSignedUp', 'RequestTimeTooSkewed', 'SignatureDoesNotMatch'
    ];
    const listOfNotFound = [
        'NoSuchBucket', 'NoSuchKey', 'NotFound'
    ];
    return async (err: any): Promise<never> => {
        console.debug(`error from ${uri} `, err);
        if (err instanceof FileSystemError) {
            return Promise.reject(err);
        }
        // eslint-disable-next-line eqeqeq
        if (listOfForbidden.includes(err.name || err.code) || err.$metadata?.httpStatusCode == 403) {
            return Promise.reject(FileSystemError.NoPermissions(uri || err.message));
        }
        // eslint-disable-next-line eqeqeq
        if (listOfNotFound.includes(err.name || err.code) || err.$metadata?.httpStatusCode == 404) {
            return Promise.reject(FileSystemError.FileNotFound(uri || err.message));
        }
        return Promise.reject(err);
    };
};