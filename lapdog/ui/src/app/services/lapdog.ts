import { Injectable} from '@angular/core';
import { Http } from '@angular/http';
import { Observable } from 'rxjs';
import { shareReplay, tap, map} from 'rxjs/operators';

@Injectable()
export class LapdogService {

  private localAPI = "http://localhost:4201/api/v1/";
  private this.firecloudStatus: Observable<any> = null;

  constructor(private http: Http) {


  }

  getFirecloudStatus() {
    if (!this.firecloudStatus) {
      this.firecloudStatus = this.http.get(this.localAPI+'status')
        .pipe(
          tap(x => {console.log("Raw response:"); console.log(x.json())}),
          map(x => x.json())
        )
    }
    return this.firecloudStatus;

  }

  getWorkspaces() {
    return this.http.get(this.localAPI+'workspaces').pipe(map(x => x.json()));
  }
}
