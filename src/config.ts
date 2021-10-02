import { workspace, WorkspaceConfiguration } from "vscode";

export interface IConfig {
    readonly awsProfile: string | undefined
    readonly awsConfigFile: string | undefined
    readonly awsSharedCredentialsFile: string | undefined
    readonly awsRegion: string | undefined
}

export class Config implements IConfig {
    constructor(private configurations: WorkspaceConfiguration) {
    }
    get awsProfile(): string | undefined {
        return this.configurations.get<string>("awsProfile") || undefined;
    }
    get awsConfigFile(): string | undefined {
        return this.configurations.get<string>("awsConfigFile") || undefined;
    }
    get awsSharedCredentialsFile(): string | undefined {
        return this.configurations.get<string>("awsSharedCredentialsFile") || undefined;
    }
    get awsRegion(): string | undefined {
        return this.configurations.get<string>("awsRegion") || undefined;
    }
}
