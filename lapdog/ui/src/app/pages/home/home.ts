import {Component} from '@angular/core';
import {LapdogService} from '../../services/lapdog';
declare var $:any;
class Status {
  system: string;
  status: boolean;

  constructor(system: string, status: boolean) {
    this.system = system;
    this.status = status;
  }
}

@Component({
  selector: 'home',
  templateUrl: './home.html'
})
export class HomeComponent{

  systems: Status[];
  overall: boolean;
  workspaces: any[];


  constructor(public lapdog: LapdogService) {

  }

  ngOnInit() {
    this.lapdog.getFirecloudStatus().subscribe((response) => {
      console.log(response);
      this.systems = [];
      for(let key in response.systems)
      {
        this.systems.push(new Status(
          key,
          response.systems[key]
        ))
      }
      this.overall = !response.failed;
      $('.sidenav').sidenav()
    })

    this.lapdog.getWorkspaces().subscribe((response) => {this.workspaces=response})
  }
}
